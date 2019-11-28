package com.linkedin.paldb.impl;

import com.linkedin.paldb.api.*;
import com.linkedin.paldb.api.errors.StoreClosed;
import org.slf4j.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

public class StoreRWImpl<K,V> implements StoreRW<K,V> {

    private static final Logger log = LoggerFactory.getLogger(StoreRWImpl.class);

    private final Configuration config;
    private final File file;
    private final Map<K,V> buffer;
    private StoreReader<K,V> reader;
    private final int maxBufferSize;
    private final StampedLock lock = new StampedLock();
    private boolean opened = false;

    StoreRWImpl(Configuration config, File file) {
        this.config = PalDBConfigBuilder.create(config)
                .withEnableDuplicates(true)
                .build();
        this.file = file;
        this.reader = null;
        this.maxBufferSize = config.getInt(Configuration.WRITE_BUFFER_SIZE);
        this.buffer = new ConcurrentHashMap<>(maxBufferSize);
    }

    @Override
    public StoreInitializer<K, V> init() {
        try {
            if (reader != null) throw new IllegalStateException("Store is already initialized");
            return new RWInitializer<>(new WriterImpl<>(config, file), () -> {
                reader = new ReaderImpl<>(config, file);
                opened = true;
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class RWInitializer<K,V> implements StoreInitializer<K,V> {

        private final StoreWriter<K,V> writer;
        private final Runnable onClose;

        private RWInitializer(StoreWriter<K, V> writer, Runnable onClose) {
            this.writer = writer;
            this.onClose = onClose;
        }

        @Override
        public void put(K key, V value) {
            writer.put(key, value);
        }

        @Override
        public void close() {
            writer.close();
            onClose.run();
        }
    }

    @Override
    public void close() {
        if (!opened) return;
        if (reader != null) {
            reader.close();
        }
        opened = false;
    }

    @Override
    public V get(K key) {
        return get(key, null);
    }

    @Override
    public V get(K key, V defaultValue) {
        checkOpen();
        if (key == null) throw new NullPointerException("Key cannot be null");
        var value = buffer.get(key);
        if (value == null) {
            return readerGet(key, defaultValue);
        }
        return value == REMOVED ? null : value;
    }

    private V readerGet(K key, V defaultValue) {
        var stamp = lock.tryOptimisticRead();
        V value;
        try {
            value = reader.get(key, defaultValue);
        } catch (Exception e) {
            if (!lock.validate(stamp)) {
                log.warn("Got store closed exception because of compaction", e);
                var readStamp = lock.readLock();
                try {
                    return reader.get(key, defaultValue);
                } finally {
                    lock.unlockRead(readStamp);
                }
            }
            throw e;
        }

        if (!lock.validate(stamp)) {
            var readStamp = lock.readLock();
            try {
                return reader.get(key, defaultValue);
            } finally {
                lock.unlockRead(readStamp);
            }
        }
        return value;
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    @Override
    public synchronized void put(K key, V value) {
        checkOpen();
        buffer.put(key, value);

        if (needsCompaction()) {
            compact();
        }
    }

    private boolean needsCompaction() {
        return buffer.size() >= maxBufferSize;
    }

    private void checkOpen() {
        if (!opened) {
            throw new StoreClosed("The store is closed");
        }
    }

    private static final Object REMOVED = new Object();

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void remove(K key) {
        checkOpen();
        buffer.put(key, (V) REMOVED);
        if (needsCompaction()) {
            compact();
        }
    }

    @Override
    public synchronized void flush() {
        checkOpen();
        compact();
    }

    private void compact() {
        var stamp = lock.writeLock();
        try {
            log.info("Compacting {}, size: {}", file, file.length());
            var tempFile = Files.createTempFile("tmp_", ".paldb");
            try (var writer = new WriterImpl<>(config, tempFile.toFile())) {
                for (var keyValue : this) {
                    writer.put(keyValue.getKey(), keyValue.getValue());
                }
            }

            reader.close();
            log.info("Closed reader");
            Files.move(tempFile, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            log.info("Moved {} file to {}", tempFile, file);
            reader = new ReaderImpl<>(config, file);
            buffer.clear();
            log.info("Compaction completed for {} with size of {}", file, file.length());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public File getFile() {
        return file;
    }

    @Override
    public long size() {
        checkOpen();
        return reader.size() + buffer.size();
    }

    @Override
    public Iterable<Map.Entry<K, V>> iterable() {
        checkOpen();
        return () -> new RWEntryIterator<>(reader, buffer);
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        checkOpen();
        return new RWEntryIterator<>(reader, buffer);
    }

    @Override
    public Iterable<K> keys() {
        checkOpen();
        return () -> new RWKeyIterator<>(reader, buffer);
    }

    private static class RWEntryIterator<K,V> extends RWIterator<K,V> implements Iterator<Map.Entry<K,V>> {
        private RWEntryIterator(StoreReader<K, V> reader, Map<K, V> cache) {
            super(reader, cache);
        }

        @Override
        public Map.Entry<K,V> next() {
            if (!hasNext())
                throw new NoSuchElementException();
            checkedHasNext = null;
            return nextValue;
        }
    }

    private static class RWKeyIterator<K,V>  extends RWIterator<K,V> implements Iterator<K> {
        private RWKeyIterator(StoreReader<K, V> reader, Map<K, V> cache) {
            super(reader, cache);
        }

        @Override
        public K next() {
            if (!hasNext())
                throw new NoSuchElementException();
            checkedHasNext = null;
            return nextValue.getKey();
        }
    }

    private abstract static class RWIterator<K,V> {
        private final StoreReader<K,V> reader;
        private final Set<K> deletedKeys;
        private Iterator<Map.Entry<K,V>> iterator;
        Boolean checkedHasNext;
        private boolean startTheSecond;
        Map.Entry<K,V> nextValue;

        RWIterator(StoreReader<K, V> reader, Map<K, V> cache) {
            this.reader = reader;
            this.deletedKeys = new HashSet<>(1000);
            this.iterator = cache.entrySet().iterator();
        }

        public boolean hasNext() {
            if (checkedHasNext == null)
                doNext();
            return checkedHasNext;
        }

        void doNext() {
            if (iterator.hasNext()) {
                nextValue = iterator.next();
                if (!startTheSecond) {
                    if (nextValue.getValue() == REMOVED) {
                        deletedKeys.add(nextValue.getKey());
                        doNext();
                        return;
                    }
                } else {
                    if (deletedKeys.contains(nextValue.getKey())) {
                        doNext();
                        return;
                    }
                }
                checkedHasNext = true;
            } else if (startTheSecond)
                checkedHasNext = false;
            else {
                startTheSecond = true;
                iterator = reader.iterator();
                doNext();
            }
        }
    }
}
