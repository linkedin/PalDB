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
import com.linkedin.paldb.utils.FormatVersion;
import com.linkedin.paldb.utils.HashUtils;
import com.linkedin.paldb.utils.LongPacker;
import com.linkedin.paldb.utils.TempUtils;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Internal write implementation.
 */
public class StorageWriter {

  private final static Logger LOGGER = Logger.getLogger(StorageWriter.class.getName());
  // Configuration
  private final Configuration config;
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
  private int keyCount;
  private int[] keyCounts;
  // Number of values
  private int valueCount;
  // Number of collisions
  private int collisions;

  private HashUtils hashUtils;

  StorageWriter(Configuration configuration, OutputStream stream) {
    config = configuration;
    loadFactor = config.getDouble(Configuration.LOAD_FACTOR);
    if (loadFactor <= 0.0 || loadFactor >= 1.0) {
      throw new IllegalArgumentException("Illegal load factor = " + loadFactor + ", should be between 0.0 and 1.0.");
    }

    // Create temp path folder
    tempFolder = TempUtils.createTempDir("paldbtempwriter");
    tempFolder.deleteOnExit();
    LOGGER.log(Level.INFO, "Creating temporary folder at {0}", tempFolder.toString());
    outputStream = stream instanceof BufferedOutputStream ? stream : new BufferedOutputStream(stream);
    indexStreams = new DataOutputStream[0];
    dataStreams = new DataOutputStream[0];
    indexFiles = new File[0];
    dataFiles = new File[0];
    lastValues = new byte[0][];
    lastValuesLength = new int[0];
    dataLengths = new long[0];
    maxOffsetLengths = new int[0];
    keyCounts = new int[0];
    hashUtils = new HashUtils();
  }

  public void put(byte[] key, byte[] value)
      throws IOException {
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
    int offsetLength = LongPacker.packLong(indexStream, dataLength);
    maxOffsetLengths[keyLength] = Math.max(offsetLength, maxOffsetLengths[keyLength]);

    // Write if data is not the same
    if (!sameValue) {
      // Get stream
      DataOutputStream dataStream = getDataStream(keyLength);

      // Write size and value
      int valueSize = LongPacker.packInt(dataStream, value.length);
      dataStream.write(value);

      // Update data length
      dataLengths[keyLength] += valueSize + value.length;

      // Update last value
      lastValues[keyLength] = value;
      lastValuesLength[keyLength] = valueSize + value.length;

      valueCount++;
    }

    keyCount++;
    keyCounts[keyLength]++;
  }

  public void close()
      throws IOException {
    // Close the data and index streams
    for (DataOutputStream dos : dataStreams) {
      if (dos != null) {
        dos.close();
      }
    }
    for (DataOutputStream dos : indexStreams) {
      if (dos != null) {
        dos.close();
      }
    }

    // Stats
    LOGGER.log(Level.INFO, "Number of keys: {0}", keyCount);
    LOGGER.log(Level.INFO, "Number of values: {0}", valueCount);

    // Prepare files to merge
    List<File> filesToMerge = new ArrayList<File>();

    try {

      //Write metadata file
      File metadataFile = new File(tempFolder, "metadata.dat");
      metadataFile.deleteOnExit();
      FileOutputStream metadataOututStream = new FileOutputStream(metadataFile);
      DataOutputStream metadataDataOutputStream = new DataOutputStream(metadataOututStream);
      writeMetadata(metadataDataOutputStream);
      metadataDataOutputStream.close();
      metadataOututStream.close();
      filesToMerge.add(metadataFile);

      // Build index file
      for (int i = 0; i < indexFiles.length; i++) {
        if (indexFiles[i] != null) {
          filesToMerge.add(buildIndex(i));
        }
      }

      // Stats collisions
      LOGGER.log(Level.INFO, "Number of collisions: {0}", collisions);

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

  private void writeMetadata(DataOutputStream dataOutputStream)
      throws IOException {
    //Write format version
    dataOutputStream.writeUTF(FormatVersion.getLatestVersion().name());

    //Write time
    dataOutputStream.writeLong(System.currentTimeMillis());

    //Prepare
    int keyLengthCount = getNumKeyCount();
    int maxKeyLength = keyCounts.length - 1;

    //Write size (number of keys)
    dataOutputStream.writeInt(keyCount);

    //Write the number of different key length
    dataOutputStream.writeInt(keyLengthCount);

    //Write the max value for keyLength
    dataOutputStream.writeInt(maxKeyLength);

    // For each keyLength
    long datasLength = 0l;
    for (int i = 0; i < keyCounts.length; i++) {
      if (keyCounts[i] > 0) {
        // Write the key length
        dataOutputStream.writeInt(i);

        // Write key count
        dataOutputStream.writeInt(keyCounts[i]);

        // Write slot count
        int slots = (int) Math.round(keyCounts[i] / loadFactor);
        dataOutputStream.writeInt(slots);

        // Write slot size
        int offsetLength = maxOffsetLengths[i];
        dataOutputStream.writeInt(i + offsetLength);

        // Write index offset
        dataOutputStream.writeInt((int) indexesLength);

        // Increment index length
        indexesLength += (i + offsetLength) * slots;

        // Write data length
        dataOutputStream.writeLong(datasLength);

        // Increment data length
        datasLength += dataLengths[i];
      }
    }

    //Write serializers
    try {
      Serializers.serialize(dataOutputStream, config.getSerializers());
    } catch (Exception e) {
      throw new RuntimeException();
    }

    //Write the position of the index and the data
    int indexOffset = dataOutputStream.size() + (Integer.SIZE / Byte.SIZE) + (Long.SIZE / Byte.SIZE);
    dataOutputStream.writeInt(indexOffset);
    dataOutputStream.writeLong(indexOffset + indexesLength);
  }

  private File buildIndex(int keyLength)
      throws IOException {
    long count = keyCounts[keyLength];
    int slots = (int) Math.round(count / loadFactor);
    int offsetLength = maxOffsetLengths[keyLength];
    int slotSize = keyLength + offsetLength;

    // Init index
    File indexFile = new File(tempFolder, "index" + keyLength + ".dat");
    RandomAccessFile indexAccessFile = new RandomAccessFile(indexFile, "rw");
    try {
      indexAccessFile.setLength(slots * slotSize);
      FileChannel indexChannel = indexAccessFile.getChannel();
      MappedByteBuffer byteBuffer = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexAccessFile.length());

      // Init reading stream
      File tempIndexFile = indexFiles[keyLength];
      DataInputStream tempIndexStream = new DataInputStream(new BufferedInputStream(new FileInputStream(tempIndexFile)));
      try {
        byte[] keyBuffer = new byte[keyLength];
        byte[] slotBuffer = new byte[slotSize];
        byte[] offsetBuffer = new byte[offsetLength];

        // Read all keys
        for (int i = 0; i < count; i++) {
          // Read key
          tempIndexStream.readFully(keyBuffer);

          // Read offset
          long offset = LongPacker.unpackLong(tempIndexStream);

          // Hash
          long hash = (long) hashUtils.hash(keyBuffer);

          boolean collision = false;
          for (int probe = 0; probe < count; probe++) {
            int slot = (int) ((hash + probe) % slots);
            byteBuffer.position(slot * slotSize);
            byteBuffer.get(slotBuffer);

            long found = LongPacker.unpackLong(slotBuffer, keyLength);
            if (found == 0) {
              // The spot is empty use it
              byteBuffer.position(slot * slotSize);
              byteBuffer.put(keyBuffer);
              int pos = LongPacker.packLong(offsetBuffer, offset);
              byteBuffer.put(offsetBuffer, 0, pos);
              break;
            } else {
              collision = true;
              // Check for duplicates
              if (Arrays.equals(keyBuffer, Arrays.copyOf(slotBuffer, keyLength))) {
                throw new RuntimeException(
                        String.format("A duplicate key has been found for for key bytes %s", Arrays.toString(keyBuffer)));
              }
            }
          }

          if (collision) {
            collisions++;
          }
        }

        String msg = "  Max offset length: " + offsetLength + " bytes" +
                "\n  Slot size: " + slotSize + " bytes";

        LOGGER.log(Level.INFO, "Built index file {0}\n" + msg, indexFile.getName());
      } finally {
        // Close input
        tempIndexStream.close();

        // Close index and make sure resources are liberated
        indexChannel.close();
        indexChannel = null;
        byteBuffer = null;

        // Delete temp index file
        if (tempIndexFile.delete()) {
          LOGGER.log(Level.INFO, "Temporary index file {0} has been deleted", tempIndexFile.getName());
        }
      }
    } finally{
      indexAccessFile.close();
      indexAccessFile = null;
      System.gc();
    }

    return indexFile;
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
    LOGGER.log(Level.INFO, "Total expected store size is {0} Mb",
        new DecimalFormat("#,##0.0").format(totalSize / (1024 * 1024)));
    LOGGER.log(Level.INFO, "Usable free space on the system is {0} Mb",
        new DecimalFormat("#,##0.0").format(usableSpace / (1024 * 1024)));
    if (totalSize / (double) usableSpace >= 0.66) {
      throw new RuntimeException("Aborting because there isn' enough free disk space");
    }
  }

  //Merge files to the provided fileChannel
  private void mergeFiles(List<File> inputFiles, OutputStream outputStream)
      throws IOException {
    long startTime = System.nanoTime();

    //Merge files
    for (File f : inputFiles) {
      if (f.exists()) {
        FileInputStream fileInputStream = new FileInputStream(f);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        try {
          LOGGER.log(Level.INFO, "Merging {0} size={1}", new Object[]{f.getName(), f.length()});

          byte[] buffer = new byte[8192];
          int length;
          while ((length = bufferedInputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, length);
          }
        } finally {
          bufferedInputStream.close();
          fileInputStream.close();
        }
      } else {
        LOGGER.log(Level.INFO, "Skip merging file {0} because it doesn't exist", f.getName());
      }
    }

    LOGGER.log(Level.INFO, "Time to merge {0} s", ((System.nanoTime() - startTime) / 1000000000.0));
  }

  //Cleanup files
  private void cleanup(List<File> inputFiles) {
    for (File f : inputFiles) {
      if (f.exists()) {
        if (f.delete()) {
          LOGGER.log(Level.INFO, "Deleted temporary file {0}", f.getName());
        }
      }
    }
    if (tempFolder.delete()) {
      LOGGER.log(Level.INFO, "Deleted temporary folder at {0}", tempFolder.getAbsolutePath());
    }
  }

  //Get the data stream for the specified keyLength, create it if needed
  private DataOutputStream getDataStream(int keyLength)
      throws IOException {
    // Resize array if necessary
    if (dataStreams.length <= keyLength) {
      dataStreams = Arrays.copyOf(dataStreams, keyLength + 1);
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
  private DataOutputStream getIndexStream(int keyLength)
      throws IOException {
    // Resize array if necessary
    if (indexStreams.length <= keyLength) {
      indexStreams = Arrays.copyOf(indexStreams, keyLength + 1);
      indexFiles = Arrays.copyOf(indexFiles, keyLength + 1);
      keyCounts = Arrays.copyOf(keyCounts, keyLength + 1);
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
      indexFiles[keyLength] = file;

      dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
      indexStreams[keyLength] = dos;

      dataLengths[keyLength]++;
    }
    return dos;
  }

  private int getNumKeyCount() {
    int res = 0;
    for (int i = 0; i < keyCounts.length; i++) {
      if (keyCounts[i] != 0) {
        res++;
      }
    }
    return res;
  }
}
