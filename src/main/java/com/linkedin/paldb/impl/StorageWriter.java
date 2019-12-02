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
import com.linkedin.paldb.api.errors.*;
import com.linkedin.paldb.utils.*;
import org.slf4j.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.*;

import static com.linkedin.paldb.impl.StorageSerialization.*;
import static com.linkedin.paldb.utils.DataInputOutput.*;

/**
 * Internal write implementation.
 */
public class StorageWriter {

  private static final Logger log = LoggerFactory.getLogger(StorageWriter.class);
  // Configuration
  private final Configuration<?,?> config;
  private final double loadFactor;
  // Output
  private final File tempFolder;
  private final OutputStream outputStream;
  // Index stream
  private File[] indexFiles;
  private DataOutputStream[] indexStreams;
  // Data stream
  private File[] dataFiles;
  private DataOutputStream[] dataStreams;
  // Cache last value
  private byte[][] lastValues;
  private int[] lastValuesLength;
  // Data length
  private long[] dataLengths;
  // Index length
  private long indexesLength;
  // Max offset length
  private int[] maxOffsetLengths;
  // Number of keys
  private long keyCount;
  private long[] keyCounts;
  private long[] actualKeyCounts;
  // Number of values
  private long valueCount;
  // Number of collisions
  private int collisions;

  private final long segmentSize;
  private final boolean duplicatesEnabled;

  StorageWriter(Configuration<?,?> configuration, OutputStream stream) {
    config = configuration;
    loadFactor = config.getDouble(Configuration.LOAD_FACTOR);
    if (loadFactor <= 0.0 || loadFactor >= 1.0) {
      throw new IllegalArgumentException("Illegal load factor = " + loadFactor + ", should be between 0.0 and 1.0.");
    }

    // Create temp path folder
    tempFolder = FileUtils.createTempDir("paldbtempwriter");
    tempFolder.deleteOnExit();
    log.info("Creating temporary folder at {}", tempFolder);
    outputStream = stream instanceof BufferedOutputStream ? stream : new BufferedOutputStream(stream);
    indexStreams = new DataOutputStream[0];
    dataStreams = new DataOutputStream[0];
    indexFiles = new File[0];
    dataFiles = new File[0];
    lastValues = new byte[0][];
    lastValuesLength = new int[0];
    dataLengths = new long[0];
    maxOffsetLengths = new int[0];
    keyCounts = new long[0];
    actualKeyCounts = new long[0];
    segmentSize = config.getLong(Configuration.MMAP_SEGMENT_SIZE);
    duplicatesEnabled = config.getBoolean(Configuration.ALLOW_DUPLICATES);
  }

  private static final byte[] REMOVED_BYTES = new byte[] {REMOVED_ID};

  public void put(byte[] key, byte[] value) throws IOException {
    int keyLength = key.length;

    //Get the Output stream for that keyLength, each key length has its own file
    DataOutputStream indexStream = getIndexStream(keyLength);

    // Write key
    indexStream.write(key);

    // Check if the value is identical to the last inserted
    byte[] lastValue = lastValues[keyLength];
    boolean sameValue = lastValue != null && Arrays.equals(value, lastValue);

    // Get data stream and length
    long dataLength = dataLengths[keyLength];
    if (sameValue) {
      dataLength -= lastValuesLength[keyLength];
    }

    // Write offset and record max offset length
    int offsetDataLength = LongPacker.packLong(indexStream, dataLength);
    maxOffsetLengths[keyLength] = Math.max(offsetDataLength, maxOffsetLengths[keyLength]);
    byte[] val;
    if (value == null) {
      val = REMOVED_BYTES;
      LongPacker.packInt(indexStream, REMOVED_ID);
    } else {
      val = value;
      LongPacker.packInt(indexStream, 0);
    }

    // Write if data is not the same
    if (!sameValue) {
      // Get stream
      DataOutputStream dataStream = getDataStream(keyLength);

      // Write size and value
      int valueSize = LongPacker.packInt(dataStream, val.length);
      dataStream.write(val);

      // Update data length
      dataLengths[keyLength] += valueSize + val.length;

      // Update last value
      lastValues[keyLength] = val;
      lastValuesLength[keyLength] = valueSize + val.length;

      valueCount++;
    }

    keyCount++;
    keyCounts[keyLength]++;
    actualKeyCounts[keyLength]++;
  }

  public void close() throws IOException {
    // Close the data and index streams
    for (var dos : dataStreams) {
      if (dos != null) {
        dos.close();
      }
    }

    for (var dos : indexStreams) {
      if (dos != null) {
        dos.close();
      }
    }

    // Stats
    log.info("Number of keys: {}", keyCount);
    log.info("Number of values: {}", valueCount);

    var bloomFilter = config.getBoolean(Configuration.BLOOM_FILTER_ENABLED) ?
            new BloomFilter(keyCount, config.getDouble(Configuration.BLOOM_FILTER_ERROR_FACTOR, 0.01)) :
            null;


    // Prepare files to merge
    List<File> filesToMerge = new ArrayList<>();
    try {
      // Build index file
      for (int i = 0; i < indexFiles.length; i++) {
        if (indexFiles[i] != null) {
          filesToMerge.add(buildIndex(i, bloomFilter));
        }
      }

      //Write metadata file
      File metadataFile = new File(tempFolder, "metadata.dat");
      metadataFile.deleteOnExit();

      try (var metadataOutputStream = new RandomAccessFile(metadataFile, "rw")) {
        writeMetadata(metadataOutputStream, bloomFilter);
      }

      filesToMerge.add(0, metadataFile);

      // Stats collisions
      log.info("Number of collisions: {}", collisions);

      // Add data files
      for (File dataFile : dataFiles) {
        if (dataFile != null) {
          filesToMerge.add(dataFile);
        }
      }

      // Merge and write to output
      checkFreeDiskSpace(filesToMerge);
      mergeFiles(filesToMerge, outputStream);
    } finally {
      outputStream.close();
      cleanup(filesToMerge);
    }
  }

  private void writeMetadata(RandomAccessFile dataOutputStream, BloomFilter bloomFilter) throws IOException {
    //Write format version
    dataOutputStream.writeUTF(FormatVersion.getLatestVersion().name());

    //Write time
    dataOutputStream.writeLong(System.currentTimeMillis());

    //Prepare
    int keyLengthCount = getNumKeyCount();
    int maxKeyLength = keyCounts.length - 1;

    //Write size (number of keys)
    dataOutputStream.writeLong(keyCount);

    //write bloom filter bit size
    dataOutputStream.writeInt(bloomFilter != null ? bloomFilter.bitSize() : 0);
    //write bloom filter long array size
    dataOutputStream.writeInt(bloomFilter != null ? bloomFilter.bits().length : 0);
    //write bloom filter hash functions
    dataOutputStream.writeInt(bloomFilter != null ? bloomFilter.hashFunctions() : 0);
    //write bloom filter bits
    if (bloomFilter != null) {
      for (final long bit : bloomFilter.bits()) {
        dataOutputStream.writeLong(bit);
      }
    }

    //Write the number of different key length
    dataOutputStream.writeInt(keyLengthCount);

    //Write the max value for keyLength
    dataOutputStream.writeInt(maxKeyLength);

    // For each keyLength
    long datasLength = 0L;
    for (int i = 0; i < keyCounts.length; i++) {
      if (keyCounts[i] > 0) {
        // Write the key length
        dataOutputStream.writeInt(i);

        // Write key count
        dataOutputStream.writeLong(keyCounts[i]);
        dataOutputStream.writeLong(actualKeyCounts[i]);

        // Write slot count
        long slots = Math.round(keyCounts[i] / loadFactor);
        dataOutputStream.writeLong(slots);

        // Write slot size
        int offsetLength = maxOffsetLengths[i];
        dataOutputStream.writeInt(i + offsetLength);

        // Write index offset
        dataOutputStream.writeLong(indexesLength);

        // Increment index length
        indexesLength += (i + offsetLength) * slots;

        // Write data length
        dataOutputStream.writeLong(datasLength);

        // Increment data length
        datasLength += dataLengths[i];
      }
    }

    //Write the position of the index and the data
    long indexOffset = dataOutputStream.getFilePointer() + (Long.SIZE / Byte.SIZE) + (Long.SIZE / Byte.SIZE);
    dataOutputStream.writeLong(indexOffset);
    //data offset
    dataOutputStream.writeLong(indexOffset + indexesLength);
  }

  private MappedByteBuffer[] initIndexBuffers(FileChannel channel, long indexSize) {
    int bufIdx = 0;
    int bufArraySize = (int) (indexSize / segmentSize) + ((indexSize % segmentSize != 0) ? 1 : 0);
    var result = new MappedByteBuffer[bufArraySize];
    try {
      for (long offset = 0; offset < indexSize; offset += segmentSize) {
        long remainingFileSize = indexSize - offset;
        long thisSegmentSize = Math.min(segmentSize, remainingFileSize);
        result[bufIdx++] = channel.map(FileChannel.MapMode.READ_WRITE, offset, thisSegmentSize);
      }
      return result;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private File buildIndex(int keyLength, BloomFilter bloomFilter) throws IOException {
    long count = keyCounts[keyLength];
    long duplicateCount = 0L;
    long numSlots = Math.round(count / loadFactor);
    int offsetLength = maxOffsetLengths[keyLength];
    int slotSize = keyLength + offsetLength;
    // Init index
    var indexFile = new File(tempFolder, "index" + keyLength + ".dat");
    try (var indexAccessFile = new RandomAccessFile(indexFile, "rw")) {
      indexAccessFile.setLength(numSlots * slotSize);
      try (var indexChannel = indexAccessFile.getChannel()) {
        var indexBuffers = initIndexBuffers(indexChannel, indexAccessFile.length());
      // Init reading stream
        File tempIndexFile = indexFiles[keyLength];
        try (DataInputStream tempIndexStream = new DataInputStream(new BufferedInputStream(new FileInputStream(tempIndexFile)))) {
          byte[] keyBuffer = new byte[keyLength];
          byte[] slotBuffer = new byte[slotSize];
          byte[] offsetBuffer = new byte[offsetLength];

          // Read all keys
          for (long i = 0; i < count; i++) {
            // Read key
            tempIndexStream.readFully(keyBuffer);

            // Read offset
            long offsetData = LongPacker.unpackLong(tempIndexStream);
            int head = LongPacker.unpackInt(tempIndexStream);
            boolean isRemoved = head == REMOVED_ID;
            // Hash
            long hash = Murmur3.hash(keyBuffer);
            if (bloomFilter != null) {
              bloomFilter.add(keyBuffer);
            }
            boolean collision = false;
            for (long probe = 0; probe < count; probe++) {
              long slot = ((hash + probe) % numSlots);
              getFromBuffers(indexBuffers, slot * slotSize, slotBuffer, slotSize, segmentSize);
              long found = LongPacker.unpackLong(slotBuffer, keyLength);
              if (found == 0L) {

                if (isRemoved) {
                  duplicateCount++;
                  break;
                }
                // The spot is empty use it
                putIntoBuffers(indexBuffers, slot * slotSize, keyBuffer, keyBuffer.length, segmentSize);
                int pos = LongPacker.packLong(offsetBuffer, offsetData);
                putIntoBuffers(indexBuffers, (slot * slotSize) + keyBuffer.length, offsetBuffer, pos, segmentSize);
                break;
              } else {
                collision = true;
                // Check for duplicates
                if (isKey(slotBuffer, keyBuffer)) {
                  if (isRemoved || duplicatesEnabled) {
                    putIntoBuffers(indexBuffers, slot * slotSize, keyBuffer, keyBuffer.length, segmentSize);
                    int pos = LongPacker.packLong(offsetBuffer, offsetData);
                    putIntoBuffers(indexBuffers, (slot * slotSize) + keyBuffer.length, offsetBuffer, pos, segmentSize);
                    duplicateCount++;
                    if (isRemoved) duplicateCount++;
                    break;
                  } else {
                    throw new DuplicateKeyException(
                            String.format("A duplicate key has been found for key bytes %s", Arrays.toString(keyBuffer)));
                  }
                }
              }
            }

            if (collision) {
              collisions++;
            }
          }

          String msg = "  Max offset length: " + offsetLength + " bytes" +
                  "\n  Slot size: " + slotSize + " bytes";

          log.info("Built index file {} \n{}", indexFile.getName(), msg);

        } finally {
          unmap(indexBuffers);
          if (tempIndexFile.delete()) {
            log.info("Temporary index file {} has been deleted", tempIndexFile.getName());
          }
        }
      }
    } catch (Exception e) {
      Files.deleteIfExists(indexFile.toPath());
      throw e;
    }
    keyCount -= duplicateCount;
    actualKeyCounts[keyLength] -= duplicateCount;
    return indexFile;
  }

  private static boolean isKey(byte[] slotBuffer, byte[] key) {
    return Arrays.compare(slotBuffer, 0, key.length, key, 0, key.length) == 0;
  }

  //Fail if the size of the expected store file exceed 2/3rd of the free disk space
  private void checkFreeDiskSpace(List<File> inputFiles) {
    //Check for free space
    long usableSpace = 0;
    long totalSize = 0;
    for (File f : inputFiles) {
      if (f.exists()) {
        totalSize += f.length();
        usableSpace = f.getUsableSpace();
      }
    }
    log.info("Total expected store size is {} Mb",
        new DecimalFormat("#,##0.0").format(totalSize / (1024 * 1024)));
    log.info("Usable free space on the system is {} Mb",
        new DecimalFormat("#,##0.0").format(usableSpace / (1024 * 1024)));
    if (totalSize / (double) usableSpace >= 0.66) {
      throw new OutOfDiskSpace("Aborting because there isn't enough free disk space");
    }
  }

  //Merge files to the provided fileChannel
  private void mergeFiles(List<File> inputFiles, OutputStream outputStream) throws IOException {
    long startTime = System.nanoTime();
    //Merge files
    for (File f : inputFiles) {
      if (f.exists()) {
        log.info("Merging {} size={}", f.getName(), f.length());
        Files.copy(f.toPath(), outputStream);
      } else {
        log.info("Skip merging file {} because it doesn't exist", f.getName());
      }
    }
    log.debug("Time to merge {} s", ((System.nanoTime() - startTime) / 1000000000.0));
  }

  private void cleanup(List<File> inputFiles) {
    for (File f : inputFiles) {
      try {
        if (Files.deleteIfExists(f.toPath())) {
          log.info("Deleted temporary file {}", f.getName());
        }
      } catch (IOException e) {
        log.error("Cannot delete file " + f, e);
      }
    }

    if (FileUtils.deleteDirectory(tempFolder)) {
      log.info("Deleted temporary folder at {}", tempFolder.getAbsolutePath());
    }
  }

  //Get the data stream for the specified keyLength, create it if needed
  private DataOutputStream getDataStream(int keyLength)
      throws IOException {
    // Resize array if necessary
    if (dataStreams.length <= keyLength) {
      var copyOfDataStreams = Arrays.copyOf(dataStreams, keyLength + 1);
      Arrays.fill(dataStreams, null);
      dataStreams = null;
      dataStreams = copyOfDataStreams;
      dataFiles = Arrays.copyOf(dataFiles, keyLength + 1);
    }

    DataOutputStream dos = dataStreams[keyLength];
    if (dos == null) {
      File file = new File(tempFolder, "data" + keyLength + ".dat");
      file.deleteOnExit();
      dataFiles[keyLength] = file;

      dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
      dataStreams[keyLength] = dos;

      // Write one byte so the zero offset is reserved
      dos.writeByte(0);
    }
    return dos;
  }

  //Get the index stream for the specified keyLength, create it if needed
  private DataOutputStream getIndexStream(int keyLength) throws IOException {
    // Resize array if necessary
    if (indexStreams.length <= keyLength) {
      var copyOfIndexStreams = Arrays.copyOf(indexStreams, keyLength + 1);
      Arrays.fill(indexStreams, null);
      indexStreams = null;
      indexStreams = copyOfIndexStreams;
      indexFiles = Arrays.copyOf(indexFiles, keyLength + 1);
      keyCounts = Arrays.copyOf(keyCounts, keyLength + 1);
      actualKeyCounts = Arrays.copyOf(actualKeyCounts, keyLength + 1);
      maxOffsetLengths = Arrays.copyOf(maxOffsetLengths, keyLength + 1);
      lastValues = Arrays.copyOf(lastValues, keyLength + 1);
      lastValuesLength = Arrays.copyOf(lastValuesLength, keyLength + 1);
      dataLengths = Arrays.copyOf(dataLengths, keyLength + 1);
    }

    // Get or create stream
    DataOutputStream dos = indexStreams[keyLength];
    if (dos == null) {
      File file = new File(tempFolder, "temp_index" + keyLength + ".dat");
      file.deleteOnExit();
      dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
      indexFiles[keyLength] = file;
      indexStreams[keyLength] = dos;
      dataLengths[keyLength]++;
    }
    return dos;
  }

  private int getNumKeyCount() {
    int res = 0;
    for (final long count : keyCounts) {
      if (count != 0L) {
        res++;
      }
    }
    return res;
  }
}
