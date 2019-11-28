package com.linkedin.paldb.impl;

import com.linkedin.paldb.api.*;
import com.linkedin.paldb.api.errors.*;
import com.linkedin.paldb.utils.TempUtils;
import org.slf4j.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.*;
import java.util.stream.*;

public class StoreRWImpl<K,V> implements StoreRW<K,V> {

    private static final Logger log = LoggerFactory.getLogger(StoreRWImpl.class);

    private final Configuration<K,V> config;
    private final File file;
    private final LinkedHashMap<K,V> buffer;
    private final AtomicReference<ReaderImpl<K,V>> reader;
    private final int maxBufferSize;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final List<OnStoreCompacted<K,V>> listeners;
    private boolean opened = false;
    private volatile boolean compacting = false;

    StoreRWImpl(Configuration<K,V> config, File file) {
        this.config = PalDBConfigBuilder.create(config)
                .withEnableDuplicates(true)
                .build();
        this.file = file;
        this.reader = new AtomicReference<>();
        this.maxBufferSize = config.getInt(Configuration.WRITE_BUFFER_SIZE);
        this.buffer = new LinkedHashMap<>(maxBufferSize);
        this.listeners = config.getStoreCompactedEventListeners();
    }

    @Override
    public StoreInitializer<K, V> init() {
        try {
            if (reader.get() != null) throw new IllegalStateException("Store is already initialized");
            final var fileToInit = resolveInitFile();
            if (!file.equals(fileToInit)) {
                reader.set(new ReaderImpl<>(config, file));
            }
            return new RWInitializer<>(new WriterImpl<>(config, fileToInit), () -> {
                var initReader = new ReaderImpl<K,V>(config, fileToInit);
                reader.set(reader.get() != null ? merge(reader.get(), initReader) : initReader);
                if (!file.equals(fileToInit)) {
                    try {
                        Files.deleteIfExists(fileToInit.toPath());
                    } catch (IOException e) {
                        log.error("Unable to delete temp file " + fileToInit, e);
                    }
                }
                opened = true;
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private File resolveInitFile() {
        return file.exists() && file.length() > 0L ?
                TempUtils.createTempFile("writer_", ".paldb") :
                file;
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
    public V get(K key) {
        return get(key, null);
    }

    @Override
    public V get(K key, V defaultValue) {
        checkOpen();
        if (key == null) throw new NullPointerException("Key cannot be null");

        rwLock.readLock().lock();
        try {
            var value = buffer.get(key);
            if (value == null) {
                return reader.get().get(key, defaultValue);
            }
            return value == REMOVED ? null : value;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Configuration<K,V> getConfiguration() {
        return config;
    }

    @Override
    public void put(K key, V value) {
        checkOpen();
        rwLock.writeLock().lock();
        try {
            buffer.put(key, value);
        } finally {
            rwLock.writeLock().unlock();
        }

        if (needsCompaction()) {
            compact();
        }
    }

    private boolean needsCompaction() {
        rwLock.readLock().lock();
        try {
            return buffer.size() >= maxBufferSize && !compacting;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void checkOpen() {
        if (!opened) {
            throw new StoreClosed("The store is closed");
        }
    }

    private static final Object REMOVED = new Object();

    @SuppressWarnings("unchecked")
    @Override
    public void remove(K key) {
        checkOpen();
        rwLock.writeLock().lock();
        try {
            buffer.put(key, (V) REMOVED);
        } finally {
            rwLock.writeLock().unlock();
        }

        if (needsCompaction()) {
            compact();
        }
    }

    @Override
    public synchronized void flush() {
        checkOpen();
        compact().join();
    }

    private ReaderImpl<K,V> merge(ReaderImpl<K,V> reader1, ReaderImpl<K,V> reader2) {
        if (reader1.equals(reader2)) return reader1;
        if (reader1.getFile().equals(reader2.getFile())) return reader1;

        log.info("Merging {} into {}", reader2, reader1);
        var tempFile = TempUtils.createTempFile("tmp_", ".paldb");
        try (var writer = new WriterImpl<K,V>(reader1.getConfiguration(), tempFile);
             var r1 = reader1;
             var r2 = reader2) {

            try (var stream = r1.stream()) {
                stream.forEach(keyValue -> writer.put(keyValue.getKey(), keyValue.getValue()));
            }

            try (var stream = r2.stream()) {
                stream.forEach(keyValue -> writer.put(keyValue.getKey(), keyValue.getValue()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        try {
            Files.move(tempFile.toPath(), reader1.getFile().toPath(), StandardCopyOption.REPLACE_EXISTING);
            log.info("Moved {} file to {}", tempFile, reader1.getFile());
            return new ReaderImpl<>(reader1.getConfiguration(), reader1.getFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private LinkedHashMap<K,V> copyBuffer() {
        rwLock.readLock().lock();
        try {
            return new LinkedHashMap<>(buffer);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private Map.Entry<K,V> lastEntry(LinkedHashMap<K,V> map) {
        Map.Entry<K,V> result = null;

        for (var kvEntry : map.entrySet()) {
            result = kvEntry;
        }

        return result;
    }

    @Override
    public synchronized CompletableFuture<Map.Entry<K,V>> compact() {
        if (compacting) return CompletableFuture.failedFuture(new StoreIsCompacting("Store is currently compacting"));
        final var entries = copyBuffer();
        if (entries.isEmpty()) return CompletableFuture.completedFuture(null);
        compacting = true;
        return CompletableFuture.supplyAsync(() -> {
            try {
                var lastEntry = lastEntry(entries);
                log.info("Compacting {}, size: {}", file, file.length());
                var tempFile = TempUtils.createTempFile("tmp_", ".paldb");
                try (var writer = new WriterImpl<>(config, tempFile)) {
                    Iterable<Map.Entry<K,V>> iter = () -> new RWEntryIterator<>(reader.get(), entries, null);
                    for (var keyValue : iter) {
                        writer.put(keyValue.getKey(), keyValue.getValue());
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                rwLock.writeLock().lock();
                try {
                    var oldReader = reader.get();
                    oldReader.close();
                    log.info("Closed reader");
                    try {
                        Files.move(tempFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    log.info("Copied {} file to {}", tempFile, file);
                    reader.set(new ReaderImpl<>(config, file));

                    entries.forEach((k, v) -> buffer.computeIfPresent(k, (key, oldValue) -> {
                        if (oldValue.equals(v)) return null;
                        return oldValue;
                    }));
                } finally {
                    rwLock.writeLock().unlock();
                }

                invokeOnCompacted(lastEntry, file);

                log.info("Compaction completed for {} with size of {}", file, file.length());
                return lastEntry;
            } finally {
                compacting = false;
            }
        });
    }

    @Override
    public File getFile() {
        return file;
    }

    @Override
    public long size() {
        checkOpen();
        rwLock.readLock().lock();
        try {
            return reader.get().size() + buffer.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Stream<Map.Entry<K, V>> stream() {
        var iterator = iterator();
        return StreamSupport.stream(iterator.spliterator(), false)
                .onClose(iterator::close);
    }

    @Override
    public Stream<K> streamKeys() {
        var iterator = keys();
        return StreamSupport.stream(iterator.spliterator(), false)
                .onClose(iterator::close);
    }

    private void invokeOnCompacted(Map.Entry<K,V> lastEntry, File storeFile) {
        try {
            for (var listener : listeners) {
                listener.apply(lastEntry, storeFile);
            }
        } catch (Exception e) {
            log.error("User error after compaction", e);
        }
    }

    private RWEntryIterator<K, V> iterator() {
        checkOpen();
        return new RWEntryIterator<>(reader.get(), copyBuffer(), rwLock);
    }

    private RWKeyIterator<K,V> keys() {
        checkOpen();
        return new RWKeyIterator<>(reader.get(), copyBuffer(), rwLock);
    }

    @Override
    public void close() {
        if (!opened) return;
        if (reader.get() != null) {
            reader.get().close();
        }
        opened = false;
    }

    private static class RWEntryIterator<K,V> extends RWIterator<K,V> implements Iterator<Map.Entry<K,V>>, Iterable<Map.Entry<K,V>> {
        private RWEntryIterator(ReaderImpl<K, V> reader, Map<K, V> cache, ReentrantReadWriteLock rwLock) {
            super(reader, cache, rwLock);
        }

        @Override
        public Map.Entry<K,V> next() {
            if (!hasNext())
                throw new NoSuchElementException();
            checkedHasNext = null;
            return nextValue;
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return this;
        }
    }

    private static class RWKeyIterator<K,V>  extends RWIterator<K,V> implements Iterator<K>, Iterable<K> {
        private RWKeyIterator(ReaderImpl<K, V> reader, Map<K, V> cache, ReentrantReadWriteLock rwLock) {
            super(reader, cache, rwLock);
        }

        @Override
        public K next() {
            if (!hasNext())
                throw new NoSuchElementException();
            checkedHasNext = null;
            return nextValue.getKey();
        }

        @Override
        public Iterator<K> iterator() {
            return this;
        }
    }

    private abstract static class RWIterator<K,V> implements AutoCloseable {
        private final ReaderImpl<K,V> reader;
        private final Set<K> deletedKeys;
        private final ReentrantReadWriteLock rwLock;
        private Iterator<Map.Entry<K,V>> iterator;
        Boolean checkedHasNext;
        private boolean startTheSecond;
        Map.Entry<K,V> nextValue;

        RWIterator(ReaderImpl<K, V> reader, Map<K, V> cache, ReentrantReadWriteLock rwLock) {
            this.reader = reader;
            this.rwLock = rwLock;
            this.deletedKeys = new HashSet<>(1000);
            this.iterator = cache.entrySet().iterator();
        }

        public boolean hasNext() {
            if (checkedHasNext == null)
                doNext();
            return checkedHasNext;
        }

        @Override
        public void close() {
            if (startTheSecond && rwLock != null) {
                rwLock.readLock().unlock();
            }
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
                if (rwLock != null) {
                    rwLock.readLock().lock();
                }
                iterator = reader.iterator();
                doNext();
            }
        }
    }
}
