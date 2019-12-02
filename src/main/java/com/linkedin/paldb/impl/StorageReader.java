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
import com.linkedin.paldb.api.errors.VersionMismatch;
import com.linkedin.paldb.utils.*;
import org.slf4j.*;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.MalformedInputException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.*;

import static com.linkedin.paldb.impl.StorageSerialization.REMOVED_ID;
import static com.linkedin.paldb.utils.DataInputOutput.*;


/**
 * Internal read implementation.
 */
public class StorageReader implements Iterable<Map.Entry<byte[], byte[]>> {

  // Logger
  private static final Logger log = LoggerFactory.getLogger(StorageReader.class);
  // Buffer segment size
  private final long segmentSize;
  // Number of keys in the index
  private final long keyCount;
  // Key count for each key length
  private final long[] keyCounts;
  private final long[] actualKeyCounts;
  // Slot size for each key length
  private final int[] slotSizes;
  // Number of slots for each key length
  private final long[] slots;
  // Offset of the index for different key length
  private final long[] indexOffsets;
  // Offset of the data in the channel
  private final long dataOffset;
  // Offset of the data for different key length
  private final long[] dataOffsets;
  // Data size
  private final long dataSize;
  // FileChannel
  private final RandomAccessFile mappedFile;
  private final FileChannel channel;
  // Use MMap for data?
  private final boolean mMapData;
  // Buffers
  private final BloomFilter bloomFilter;
  private MappedByteBuffer[] indexBuffers;
  private MappedByteBuffer[] dataBuffers;

  StorageReader(Configuration<?,?> configuration, File file) throws IOException {
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
    long indexOffset;
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
        throw new VersionMismatch(
                "Version mismatch, expected was '" + FormatVersion.getLatestVersion() + "' and found '" + formatVersion
                        + "'");
      }

      //Time
      createdAt = dataInputStream.readLong();

      //Metadata counters
      keyCount = dataInputStream.readLong();

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
      indexOffsets = new long[maxKeyLength + 1];
      dataOffsets = new long[maxKeyLength + 1];
      keyCounts = new long[maxKeyLength + 1];
      actualKeyCounts = new long[maxKeyLength + 1];
      slots = new long[maxKeyLength + 1];
      slotSizes = new int[maxKeyLength + 1];

      int mSlotSize = 0;
      for (int i = 0; i < keyLengthCount; i++) {
        int keyLength = dataInputStream.readInt();

        keyCounts[keyLength] = dataInputStream.readLong();
        actualKeyCounts[keyLength] = dataInputStream.readLong();
        slots[keyLength] = dataInputStream.readLong();
        slotSizes[keyLength] = dataInputStream.readInt();
        indexOffsets[keyLength] = dataInputStream.readLong();
        dataOffsets[keyLength] = dataInputStream.readLong();

        mSlotSize = Math.max(mSlotSize, slotSizes[keyLength]);
      }
      //Read index and data offset
      indexOffset = dataInputStream.readLong() + ignoredBytes;
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
    indexBuffers = initIndexBuffers(channel, indexOffset);
    dataBuffers = initDataBuffers(channel);

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

  private MappedByteBuffer[] initIndexBuffers(FileChannel channel, long indexOffset) {
    long indexSize = dataOffset - indexOffset;
    int bufIdx = 0;
    int bufArraySize = (int) (indexSize / segmentSize) + ((indexSize % segmentSize != 0) ? 1 : 0);
    var result = new MappedByteBuffer[bufArraySize];
    try {
      for (long offset = 0; offset < indexSize; offset += segmentSize) {
        long remainingFileSize = indexSize - offset;
        long thisSegmentSize = Math.min(segmentSize, remainingFileSize);
        result[bufIdx++] = channel.map(FileChannel.MapMode.READ_ONLY, indexOffset + offset, thisSegmentSize);
      }
      return result;
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

    long numSlots = slots[keyLength];
    int slotSize = slotSizes[keyLength];
    long ixOffset = indexOffsets[keyLength];
    long dtOffset = dataOffsets[keyLength];
    long hash = Murmur3.hash(key);
    var slotBuffer = new byte[slotSize];
    for (long probe = 0; probe < numSlots; probe++) {
      long slot = ((hash + probe) % numSlots);
      getFromBuffers(indexBuffers, ixOffset + slot * slotSize, slotBuffer, slotSize, segmentSize);
      long offset = LongPacker.unpackLong(slotBuffer, keyLength);
      if (offset == 0L) {
        return null;
      }
      if (isKey(slotBuffer, key)) {
        return mMapData ? getMMapBytes(dtOffset + offset) : getDiskBytes(dtOffset + offset);
      }
    }
    return null;
  }

  private static boolean isKey(byte[] slotBuffer, byte[] key) {
    return Arrays.compare(slotBuffer, 0, key.length, key, 0, key.length) == 0;
  }

  //Close the reader channel
  public void close() throws IOException {
    channel.close();
    mappedFile.close();
    unmap(indexBuffers);
    unmap(dataBuffers);
    indexBuffers = null;
    dataBuffers = null;
  }

  public long getKeyCount() {
    return keyCount;
  }

  //Read the data at the given offset, the data can be spread over multiple data buffers
  private byte[] getMMapBytes(long offset) throws IOException {
    var buf = dataBuffers[(int) (offset / segmentSize)];
    var pos = (int) (offset % segmentSize);

    int maxLen = (int) Math.min(5, dataSize - offset);
    //Read the first 4 bytes to get the size of the data
    int size = -1;
    if (remaining(buf, pos) >= maxLen) {
      //Continuous read
      int oldPos = pos;

      //unpack int
      for (int i = 0, result = 0; i < 32; i += 7) {
        int b = buf.get(pos++) & 0xffff;
        result |= (b & 0x7F) << i;
        if ((b & 0x80) == 0) {
          size = result;
          break;
        }
      }
      if (size == -1) throw new MalformedInputException(Integer.BYTES);

      // Used in case of data is spread over multiple buffers
      offset += pos - oldPos;
    } else {
      //The size of the data is spread over multiple buffers
      int len = maxLen;
      int off = 0;

      try (var sizeBuffer = new DataInputOutput(new byte[5])) {
        while (len > 0) {
          buf = dataBuffers[(int) ( (offset + off) / segmentSize)];
          pos =  (int) ( (offset + off) % segmentSize);
          int count = Math.min(len, remaining(buf, pos));
          buf.get(pos, sizeBuffer.getBuf(), off, count);
          off += count;
          len -= count;
        }
        size = LongPacker.unpackInt(sizeBuffer);
        offset += sizeBuffer.getPos();

        buf = dataBuffers[(int) (offset / segmentSize)];
        pos =  (int) (offset % segmentSize);
      }
    }

    //Create output bytes
    byte[] res = new byte[size];

    //Check if the data is one buffer
    if (remaining(buf, pos) >= size) {
      //Continuous read
      buf.get(pos, res, 0, size);
    } else {
      int len = size;
      int off = 0;
      while (len > 0) {
        buf = dataBuffers[(int) (offset / segmentSize)];
        pos =  (int) (offset % segmentSize);
        int count = Math.min(len, remaining(buf, pos));
        buf.get(pos, res, off, count);
        offset += count;
        off += count;
        len -= count;
      }
    }

    return res;
  }

  //Get data from disk
  private synchronized byte[] getDiskBytes(long offset) throws IOException {
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

  private String formatCreatedAt(long createdAt) {
    return Instant.ofEpochMilli(createdAt).toString();
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
    private long currentIndexOffset;


    public StorageIterator(boolean value) {
      withValue = value;
      nextKeyLength();
    }

    private void nextKeyLength() {
      for (int i = currentKeyLength + 1; i < actualKeyCounts.length; i++) {
        long c = actualKeyCounts[i];
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
        long offset = 0;
        while (offset == 0) {
          getFromBuffers(indexBuffers, currentIndexOffset, currentSlotBuffer, currentSlotBuffer.length, segmentSize);
          offset = LongPacker.unpackLong(currentSlotBuffer, currentKeyLength);
          currentIndexOffset += currentSlotBuffer.length;
        }

        byte[] key = Arrays.copyOf(currentSlotBuffer, currentKeyLength);
        long valueOffset = currentDataOffset + offset;
        var value = mMapData ? getMMapBytes(valueOffset) : getDiskBytes(valueOffset);

        boolean isRemoved = (value.length > 0 && (((int) value[0] & 0xff) == REMOVED_ID));
        if (isRemoved) {
          return next();
        }

        entry.set(key, withValue ? value : null);

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
