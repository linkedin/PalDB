/*
* Copyright 2015 LinkedIn Corp. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*/

package com.linkedin.paldb.impl;

import com.linkedin.paldb.api.Configuration;
import com.linkedin.paldb.utils.*;
import org.slf4j.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.text.*;
import java.util.*;


/**
 * Internal read implementation.
 */
public class StorageReader implements Iterable<Map.Entry<byte[], byte[]>> {

  // Logger
  private static final Logger log = LoggerFactory.getLogger(StorageReader.class);
  // Buffer segment size
  private final long segmentSize;
  // Number of keys in the index
  private final int keyCount;
  // Key count for each key length
  private final int[] keyCounts;
  // Slot size for each key length
  private final int[] slotSizes;
  // Number of slots for each key length
  private final int[] slots;
  // Offset of the index for different key length
  private final int[] indexOffsets;
  // Offset of the data in the channel
  private final long dataOffset;
  // Offset of the data for different key length
  private final long[] dataOffsets;
  // Data size
  private final long dataSize;
  private final int maxSlotSize;
  // FileChannel
  private final RandomAccessFile mappedFile;
  private final FileChannel channel;
  // Use MMap for data?
  private final boolean mMapData;
  // Buffers
  private final ThreadLocal<ThreadContext> context;

  private final BloomFilter bloomFilter;

  StorageReader(Configuration configuration, File file) throws IOException {
    // File path
    // Configuration
    if (!file.exists()) {
      throw new FileNotFoundException("File " + file.getAbsolutePath() + " not found");
    }
    log.info("Opening file {}", file.getName());

    //Config
    segmentSize = configuration.getLong(Configuration.MMAP_SEGMENT_SIZE);

    // Check valid segmentSize
    if (segmentSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "The `" + Configuration.MMAP_SEGMENT_SIZE + "` setting can't be larger than 2GB");
    }

    //Open file and read metadata
    long createdAt = 0;
    FormatVersion formatVersion = null;

    // Offset of the index in the channel
    int indexOffset;
    int bloomFilterBitSize = 0;
    int bloomFilterHashFunctions = 0;

        try (FileInputStream inputStream = new FileInputStream(file);
             DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(inputStream))) {
      int ignoredBytes = -2;

      //Byte mark
      byte[] mark = FormatVersion.getPrefixBytes();
      int found = 0;
      while (found != mark.length) {
        byte b = dataInputStream.readByte();
        if (b == mark[found]) {
          found++;
        } else {
          ignoredBytes += found + 1;
          found = 0;
        }
      }

      //Version
      byte[] versionFound = Arrays.copyOf(mark, FormatVersion.getLatestVersion().getBytes().length);
      dataInputStream.readFully(versionFound, mark.length, versionFound.length - mark.length);

      formatVersion = FormatVersion.fromBytes(versionFound);
      if (formatVersion == null || !formatVersion.is(FormatVersion.getLatestVersion())) {
        throw new RuntimeException(
                "Version mismatch, expected was '" + FormatVersion.getLatestVersion() + "' and found '" + formatVersion
                        + "'");
      }

      //Time
      createdAt = dataInputStream.readLong();

      //Metadata counters
      keyCount = dataInputStream.readInt();

      //read bloom filter bit size
      bloomFilterBitSize = dataInputStream.readInt();
      //read bloom filter long array size
      int bloomFilterLength = dataInputStream.readInt();
      //read bloom filter hash functions
      bloomFilterHashFunctions = dataInputStream.readInt();
      if (bloomFilterLength > 0) {
        //read bloom filter long array
        long[] bits = new long[bloomFilterLength];
        for (int i = 0; i < bloomFilterLength; i++) {
          bits[i] = dataInputStream.readLong();
        }
        bloomFilter = new BloomFilter(bloomFilterHashFunctions, bloomFilterBitSize, bits);
      } else {
        bloomFilter = null;
      }

      // Number of different key length
      final int keyLengthCount = dataInputStream.readInt();
      // Max key length
      final int maxKeyLength = dataInputStream.readInt();

      //Read offset counts and keys
      indexOffsets = new int[maxKeyLength + 1];
      dataOffsets = new long[maxKeyLength + 1];
      keyCounts = new int[maxKeyLength + 1];
      slots = new int[maxKeyLength + 1];
      slotSizes = new int[maxKeyLength + 1];

      int mSlotSize = 0;
      for (int i = 0; i < keyLengthCount; i++) {
        int keyLength = dataInputStream.readInt();

        keyCounts[keyLength] = dataInputStream.readInt();
        slots[keyLength] = dataInputStream.readInt();
        slotSizes[keyLength] = dataInputStream.readInt();
        indexOffsets[keyLength] = dataInputStream.readInt();
        dataOffsets[keyLength] = dataInputStream.readLong();

        mSlotSize = Math.max(mSlotSize, slotSizes[keyLength]);
      }
      maxSlotSize = mSlotSize;

      //Read index and data offset
      indexOffset = dataInputStream.readInt() + ignoredBytes;
      dataOffset = dataInputStream.readLong() + ignoredBytes;
    }
    //Close metadata

    //Create Mapped file in read-only mode
    mappedFile = new RandomAccessFile(file, "r");
    channel = mappedFile.getChannel();
    long fileSize = file.length();

    //Create data buffers
    dataSize = fileSize - dataOffset;

    //Check if data size fits in memory map limit
    mMapData = configuration.getBoolean(Configuration.MMAP_DATA_ENABLED);
    context = mMapData ?
            ThreadLocal.withInitial(() -> new ThreadContext(
                    initIndexBuffer(channel, indexOffset),
                    initDataBuffers(channel),
                    new byte[maxSlotSize])) :
            ThreadLocal.withInitial(() -> new ThreadContext(initIndexBuffer(channel, indexOffset), new byte[maxSlotSize]));

    //logging
    if (log.isDebugEnabled()) {
      DecimalFormat integerFormat = new DecimalFormat("#,##0.00");
      StringBuilder statMsg = new StringBuilder("Storage metadata\n");
      statMsg.append("  Created at: ").append(formatCreatedAt(createdAt)).append("\n");
      statMsg.append("  Format version: ").append(formatVersion.name()).append("\n");
      statMsg.append("  Key count: ").append(keyCount).append("\n");
      for (int i = 0; i < keyCounts.length; i++) {
        if (keyCounts[i] > 0) {
          statMsg.append("  Key count for key length ").append(i).append(": ").append(keyCounts[i]).append("\n");
        }
      }
      statMsg.append("  Index size: ").append(integerFormat.format((dataOffset - indexOffset) / (1024.0 * 1024.0))).append(" Mb\n");
      statMsg.append("  Data size: ").append(integerFormat.format((fileSize - dataOffset) / (1024.0 * 1024.0))).append(" Mb\n");
      statMsg.append("  Bloom filter size: ").append(integerFormat.format((bloomFilterBitSize / 8.0) / (1024.0 * 1024.0))).append(" Mb\n");
      statMsg.append("  Bloom filter hashes: ").append(bloomFilterHashFunctions).append("\n");
      log.debug(statMsg.toString());
    }
  }

  private MappedByteBuffer initIndexBuffer(FileChannel channel, int indexOffset) {
    try {
      return channel.map(FileChannel.MapMode.READ_ONLY, indexOffset, dataOffset - indexOffset);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private MappedByteBuffer[] initDataBuffers(FileChannel channel)  {
    int bufArraySize = (int) (dataSize / segmentSize) + ((dataSize % segmentSize != 0) ? 1 : 0);
    MappedByteBuffer[] result = new MappedByteBuffer[bufArraySize];
    int bufIdx = 0;
    for (long offset = 0; offset < dataSize; offset += segmentSize) {
      long remainingFileSize = dataSize - offset;
      long thisSegmentSize = Math.min(segmentSize, remainingFileSize);
      try {
        result[bufIdx++] = channel.map(FileChannel.MapMode.READ_ONLY, dataOffset + offset, thisSegmentSize);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return result;
  }

  //Get the value for the given key or null
  public byte[] get(byte[] key) throws IOException {
    int keyLength = key.length;
    if (keyLength >= slots.length || keyCounts[keyLength] == 0) {
      return null;
    }

    if (bloomFilter != null && !bloomFilter.mightContain(key)) {
        return null;
    }

    var ctx = context.get();
    int numSlots = slots[keyLength];
    int slotSize = slotSizes[keyLength];
    int ixOffset = indexOffsets[keyLength];
    long dtOffset = dataOffsets[keyLength];
    long hash = HashUtils.hash(key);

    for (int probe = 0; probe < numSlots; probe++) {
      int slot = (int) ((hash + probe) % numSlots);
      ctx.indexBuffer.position(ixOffset + slot * slotSize);
      ctx.indexBuffer.get(ctx.slotBuffer, 0, slotSize);

      long offset = LongPacker.unpackLong(ctx.slotBuffer, keyLength);
      if (offset == 0L) {
        return null;
      }
      if (isKey(ctx.slotBuffer, key)) {
        return mMapData ? getMMapBytes(dtOffset + offset, ctx) : getDiskBytes(dtOffset + offset);
      }
    }
    return null;
  }

  private boolean isKey(byte[] slotBuffer, byte[] key) {
    for (int i = 0; i < key.length; i++) {
      if (slotBuffer[i] != key[i]) {
        return false;
      }
    }
    return true;
  }

  private static final class ThreadContext {
    private final MappedByteBuffer indexBuffer;
    private final MappedByteBuffer[] dataBuffers;
    private final DataInputOutput sizeBuffer = new DataInputOutput(new byte[5]);
    private final byte[] slotBuffer;

    private ThreadContext(MappedByteBuffer indexBuffer, byte[] slotBuffer) {
      this(indexBuffer, null, slotBuffer);
    }

    private ThreadContext(MappedByteBuffer indexBuffer, MappedByteBuffer[] dataBuffers, byte[] slotBuffer) {
      this.indexBuffer = indexBuffer;
      this.dataBuffers = dataBuffers;
      this.slotBuffer = slotBuffer;
    }
  }

  //Close the reader channel
  public void close() throws IOException {
    channel.close();
    mappedFile.close();
    context.remove();
  }

  public int getKeyCount() {
    return keyCount;
  }

  //Read the data at the given offset, the data can be spread over multiple data buffers
  private byte[] getMMapBytes(long offset, ThreadContext ctx) throws IOException {
    //Read the first 4 bytes to get the size of the data
    ByteBuffer buf = getDataBuffer(offset, ctx);
    int maxLen = (int) Math.min(5, dataSize - offset);

    int size;
    if (buf.remaining() >= maxLen) {
      //Continuous read
      int pos = buf.position();
      size = LongPacker.unpackInt(buf);

      // Used in case of data is spread over multiple buffers
      offset += buf.position() - pos;
    } else {
      //The size of the data is spread over multiple buffers
      int len = maxLen;
      int off = 0;
      ctx.sizeBuffer.reset();
      while (len > 0) {
        buf = getDataBuffer(offset + off, ctx);
        int count = Math.min(len, buf.remaining());
        buf.get(ctx.sizeBuffer.getBuf(), off, count);
        off += count;
        len -= count;
      }
      size = LongPacker.unpackInt(ctx.sizeBuffer);
      offset += ctx.sizeBuffer.getPos();
      buf = getDataBuffer(offset, ctx);
    }

    //Create output bytes
    byte[] res = new byte[size];

    //Check if the data is one buffer
    if (buf.remaining() >= size) {
      //Continuous read
      buf.get(res, 0, size);
    } else {
      int len = size;
      int off = 0;
      while (len > 0) {
        buf = getDataBuffer(offset, ctx);
        int count = Math.min(len, buf.remaining());
        buf.get(res, off, count);
        offset += count;
        off += count;
        len -= count;
      }
    }

    return res;
  }

  //Get data from disk
  private synchronized byte[] getDiskBytes(long offset)
      throws IOException {
    mappedFile.seek(dataOffset + offset);

    //Get size of data
    int size = LongPacker.unpackInt(mappedFile);

    //Create output bytes
    byte[] res = new byte[size];

    //Read data
    if (mappedFile.read(res) == -1) {
      throw new EOFException();
    }

    return res;
  }

  //Return the data buffer for the given position
  private ByteBuffer getDataBuffer(long index, ThreadContext ctx) {
    ByteBuffer buf = ctx.dataBuffers[(int) (index / segmentSize)];
    buf.position((int) (index % segmentSize));
    return buf;
  }

  private String formatCreatedAt(long createdAt) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd G 'at' HH:mm:ss z");
    Calendar cl = Calendar.getInstance();
    cl.setTimeInMillis(createdAt);
    return sdf.format(cl.getTime());
  }

  @Override
  public Iterator<Map.Entry<byte[], byte[]>> iterator() {
    return new StorageIterator(true);
  }

  public Iterator<Map.Entry<byte[], byte[]>> keys() {
    return new StorageIterator(false);
  }

  private class StorageIterator implements Iterator<Map.Entry<byte[], byte[]>> {

    private final FastEntry entry = new FastEntry();
    private final boolean withValue;
    private int currentKeyLength = 0;
    private byte[] currentSlotBuffer;
    private long keyIndex;
    private long keyLimit;
    private long currentDataOffset;
    private int currentIndexOffset;


    public StorageIterator(boolean value) {
      withValue = value;
      nextKeyLength();
    }

    private void nextKeyLength() {
      for (int i = currentKeyLength + 1; i < keyCounts.length; i++) {
        long c = keyCounts[i];
        if (c > 0) {
          currentKeyLength = i;
          keyLimit += c;
          currentSlotBuffer = new byte[slotSizes[i]];
          currentIndexOffset = indexOffsets[i];
          currentDataOffset = dataOffsets[i];
          break;
        }
      }
    }

    @Override
    public boolean hasNext() {
      return keyIndex < keyLimit;
    }

    @Override
    public FastEntry next() {
      try {
        var ctx = context.get();
        ctx.indexBuffer.position(currentIndexOffset);

        long offset = 0;
        while (offset == 0) {
          ctx.indexBuffer.get(currentSlotBuffer);
          offset = LongPacker.unpackLong(currentSlotBuffer, currentKeyLength);
          currentIndexOffset += currentSlotBuffer.length;
        }

        byte[] key = Arrays.copyOf(currentSlotBuffer, currentKeyLength);
        byte[] value = null;

        if (withValue) {
          long valueOffset = currentDataOffset + offset;
          value = mMapData ? getMMapBytes(valueOffset, ctx) : getDiskBytes(valueOffset);
        }

        entry.set(key, value);

        if (++keyIndex == keyLimit) {
          nextKeyLength();
        }
        return entry;
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    private class FastEntry implements Map.Entry<byte[], byte[]> {

      private byte[] key;
      private byte[] val;

      protected void set(byte[] k, byte[] v) {
        this.key = k;
        this.val = v;
      }

      @Override
      public byte[] getKey() {
        return key;
      }

      @Override
      public byte[] getValue() {
        return val;
      }

      @Override
      public byte[] setValue(byte[] value) {
        throw new UnsupportedOperationException("Not supported.");
      }
    }
  }
}
