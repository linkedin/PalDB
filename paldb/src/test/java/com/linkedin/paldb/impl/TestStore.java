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

import com.linkedin.paldb.api.*;
import com.linkedin.paldb.utils.*;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import static com.linkedin.paldb.utils.TestTempUtils.deleteDirectory;

public class TestStore {

  private Path tempDir;
  private File storeFile;

  @BeforeMethod
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("tmp");
    storeFile = Files.createTempFile(tempDir, "paldb", ".dat").toFile();
  }

  @AfterMethod
  public void cleanUp() {
    deleteDirectory(tempDir.toFile());
  }

  @Test
  public void testEmpty() {
    StoreWriter writer = PalDB.createWriter(storeFile, new Configuration());
    writer.close();

    Assert.assertTrue(storeFile.exists());

    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.size(), 0);
      Assert.assertNull(reader.get(1, null));
    }
  }

  @Test
  public void testEmptyStream() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StoreWriter writer = PalDB.createWriter(bos, new Configuration());
    writer.close();

    Assert.assertTrue(bos.toByteArray().length > 0);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    StoreReader reader = PalDB.createReader(bis, new Configuration());
    reader.close();
  }

  @Test
  public void testEmptyDefaultConfig() {
    StoreWriter writer = PalDB.createWriter(storeFile);
    writer.close();

    Assert.assertTrue(storeFile.exists());

    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile)) {
      Assert.assertEquals(reader.size(), 0);
      Assert.assertNull(reader.get(1, null));
    }
  }

  @Test
  public void testNewConfiguration() {
    Assert.assertNotNull(PalDB.newConfiguration());
  }

  @Test
  public void testNoFolder() {
    File file = new File("nofolder.store");
    file.deleteOnExit();
    StoreWriter writer = PalDB.createWriter(file, new Configuration());
    writer.close();

    Assert.assertTrue(file.exists());
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*not found.*")
  public void testReaderFileNotFound() {
    PalDB.createReader(new File("notfound"), PalDB.newConfiguration());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testReaderNullFile() {
    PalDB.createReader((File) null, PalDB.newConfiguration());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testReaderNullConfig() {
    PalDB.createReader(new File("notfound"), null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testReaderNullStream() {
    PalDB.createReader((InputStream) null, PalDB.newConfiguration());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testReaderNullConfigForStream() {
    PalDB.createReader(new InputStream() {
      @Override
      public int read()
              throws IOException {
        return 0;
      }
    }, null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testWriterNullFile() {
    PalDB.createWriter((File) null, PalDB.newConfiguration());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testWriterNullConfig() {
    PalDB.createWriter(new File("notfound"), null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testWriterNullStream() {
    PalDB.createWriter((OutputStream) null, PalDB.newConfiguration());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testWriterNullConfigForStream() {
    PalDB.createWriter(new OutputStream() {
      @Override
      public void write(int i)
              throws IOException {

      }
    }, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidSegmentSize() {
    StoreWriter writer = PalDB.createWriter(storeFile);
    writer.close();

    Configuration config = new Configuration();
    config.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(1 + (long) Integer.MAX_VALUE));
    PalDB.createReader(storeFile, config);
  }

  @Test
  public void testByteMarkEmpty() throws IOException {
    try (FileOutputStream fos = new FileOutputStream(storeFile)) {
      fos.write(12345);
      fos.write(FormatVersion.getPrefixBytes()[0]);
      fos.write(3456);
      StoreWriter writer = PalDB.createWriter(fos, new Configuration());
      writer.close();
    }

    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.size(), 0);
      Assert.assertNull(reader.get(1, null));
    }
  }

  @Test
  public void testOneKey() {
    try (StoreWriter writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.put(1, "foo");
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.size(), 1);
      Assert.assertEquals(reader.get(1), "foo");
    }
  }

  @Test
  public void testPutSerializedKey() throws IOException {
    StorageSerialization storageSerialization = new StorageSerialization(new Configuration());
    byte[] serializedKey = storageSerialization.serializeKey(1);
    byte[] serializedValue = storageSerialization.serializeValue("foo");

    try (StoreWriter writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.put(serializedKey, serializedValue);
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.size(), 1);
      Assert.assertEquals(reader.get(1), "foo");
    }
  }

  @Test
  public void testByteMarkOneKey() throws IOException {
    try (FileOutputStream fos = new FileOutputStream(storeFile);
         StoreWriter writer = PalDB.createWriter(fos, new Configuration());) {
      fos.write(12345);
      fos.write(FormatVersion.getPrefixBytes()[0]);
      fos.write(3456);

      writer.put(1, "foo");
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.size(), 1);
      Assert.assertEquals(reader.get(1), "foo");
    }
  }

  @Test
  public void testTwoFirstKeyLength() {
    Integer key1 = 1;
    Integer key2 = 245;

    //Test key length
    testKeyLength(key1, 1);
    testKeyLength(key2, 2);

    //Write
    writeStore(storeFile, new Object[]{key1, key2}, new Object[]{1, 6});

    //Read
    try (StoreReader<Integer, Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.get(key1).intValue(), 1);
      Assert.assertEquals(reader.get(key2).intValue(), 6);
      Assert.assertNull(reader.get(0, null));
      Assert.assertNull(reader.get(6, null));
      Assert.assertNull(reader.get(244, null));
      Assert.assertNull(reader.get(246, null));
      Assert.assertNull(reader.get(1245, null));
    }
  }

  @Test
  public void testKeyLengthGap() {
    Integer key1 = 1;
    Integer key2 = 2450;

    //Test key length
    testKeyLength(key1, 1);
    testKeyLength(key2, 3);

    //Write
    writeStore(storeFile, new Object[]{key1, key2}, new Object[]{1, 6});

    //Read
    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.get(key1).intValue(), 1);
      Assert.assertEquals(reader.get(key2).intValue(), 6);
      Assert.assertNull(reader.get(0, null));
      Assert.assertNull(reader.get(6, null));
      Assert.assertNull(reader.get(244, null));
      Assert.assertNull(reader.get(267, null));
      Assert.assertNull(reader.get(2449, null));
      Assert.assertNull(reader.get(2451, null));
      Assert.assertNull(reader.get(2454441, null));
    }
  }

  @Test
  public void testKeyLengthStartTwo() {
    Integer key1 = 245;
    Integer key2 = 2450;

    //Test key length
    testKeyLength(key1, 2);
    testKeyLength(key2, 3);

    //Write
    writeStore(storeFile, new Object[]{key1, key2}, new Object[]{1, 6});

    //Read
    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.get(key1).intValue(), 1);
      Assert.assertEquals(reader.get(key2).intValue(), 6);
      Assert.assertNull(reader.get(6, null));
      Assert.assertNull(reader.get(244, null));
      Assert.assertNull(reader.get(267, null));
      Assert.assertNull(reader.get(2449, null));
      Assert.assertNull(reader.get(2451, null));
      Assert.assertNull(reader.get(2454441, null));
    }
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*duplicate.*")
  public void testDuplicateKeys() {
    try (StoreWriter writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.put(0, "ABC");
      writer.put(0, "DGE");
    }
  }

  @Test
  public void testDataOnTwoBuffers() throws IOException {
    Object[] keys = new Object[]{1, 2, 3};
    Object[] values = new Object[]{GenerateTestData.generateStringData(100), GenerateTestData
        .generateStringData(10000), GenerateTestData.generateStringData(100)};

    StorageSerialization serialization = new StorageSerialization(new Configuration());
    int byteSize = serialization.serialize(values[0]).length + serialization.serialize(values[1]).length;

    //Write
    writeStore(storeFile, keys, values);

    //Read
    Configuration configuration = new Configuration();
    configuration.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(byteSize - 100));
    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, configuration)) {
      for (int i = 0; i < keys.length; i++) {
        Assert.assertEquals(reader.get((Integer) keys[i], null), values[i]);
      }
    }
  }

  @Test
  public void testDataSizeOnTwoBuffers() throws IOException {
    Object[] keys = new Object[]{1, 2, 3};
    Object[] values = new Object[]{GenerateTestData.generateStringData(100), GenerateTestData
        .generateStringData(10000), GenerateTestData.generateStringData(100)};

    StorageSerialization serialization = new StorageSerialization(new Configuration());
    byte[] b1 = serialization.serialize(values[0]);
    byte[] b2 = serialization.serialize(values[1]);
    int byteSize = b1.length + b2.length;
    int sizeSize =
        LongPacker.packInt(new DataInputOutput(), b1.length) + LongPacker.packInt(new DataInputOutput(), b2.length);

    //Write
    writeStore(storeFile, keys, values);

    //Read
    Configuration configuration = new Configuration();
    configuration.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(byteSize + sizeSize + 3));
    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, configuration)) {
      for (int i = 0; i < keys.length; i++) {
        Assert.assertEquals(reader.get((Integer) keys[i], null), values[i]);
      }
    }
  }

  @Test
  public void testReadStringToString() {
    testReadKeyToString(GenerateTestData.generateStringKeys(100));
  }

  @Test
  public void testReadIntToString() {
    testReadKeyToString(GenerateTestData.generateIntKeys(100));
  }

  @Test
  public void testReadDoubleToString() {
    testReadKeyToString(GenerateTestData.generateDoubleKeys(100));
  }

  @Test
  public void testReadLongToString() {
    testReadKeyToString(GenerateTestData.generateLongKeys(100));
  }

  @Test
  public void testReadStringToInt() {
    testReadKeyToInt(GenerateTestData.generateStringKeys(100));
  }

  @Test
  public void testReadByteToInt() {
    testReadKeyToInt(GenerateTestData.generateByteKeys(100));
  }

  @Test
  public void testReadIntToInt() {
    testReadKeyToInt(GenerateTestData.generateIntKeys(100));
  }

  @Test
  public void testReadIntToIntArray() {
    testReadKeyToIntArray(GenerateTestData.generateIntKeys(100));
  }

  @Test
  public void testReadCompoundToString() {
    testReadKeyToString(GenerateTestData.generateCompoundKeys(100));
  }

  @Test
  public void testReadCompoundByteToString() {
    testReadKeyToString(new Object[]{GenerateTestData.generateCompoundByteKey()});
  }

  @Test
  public void testReadIntToNull() {
    testReadKeyToNull(GenerateTestData.generateIntKeys(100));
  }

  @Test
  public void testReadDisk() {
    Integer[] keys = GenerateTestData.generateIntKeys(10000);
    Configuration configuration = new Configuration();

    //Write
    Object[] values = GenerateTestData.generateStringData(keys.length, 1000);
    try (StoreWriter writer = PalDB.createWriter(storeFile, configuration)) {
      writer.putAll(keys, values);
    }

    //Read
    configuration.set(Configuration.MMAP_DATA_ENABLED, "false");
    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, configuration)) {
      Assert.assertEquals(reader.size(), keys.length);

      for (int i = 0; i < keys.length; i++) {
        Integer key = keys[i];
        Object val = reader.get(key, null);
        Assert.assertNotNull(val);
        Assert.assertEquals(val, values[i]);
      }
    }
  }

  @Test
  public void testIterate() {
    Integer[] keys = GenerateTestData.generateIntKeys(100);
    String[] values = GenerateTestData.generateStringData(keys.length, 12);

    //Write
    writeStore(storeFile, keys, values);

    //Sets
    Set<Integer> keysSet = new HashSet<>(Arrays.asList(keys));
    Set<String> valuesSet = new HashSet<>(Arrays.asList(values));

    //Read
    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration())) {
      var itr = reader.iterable().iterator();
      for (int i = 0; i < keys.length; i++) {
        Assert.assertTrue(itr.hasNext());
        var entry = itr.next();
        Assert.assertNotNull(entry);
        Assert.assertTrue(keysSet.remove(entry.getKey()));
        Assert.assertTrue(valuesSet.remove(entry.getValue()));

        Object valSearch = reader.get(entry.getKey(), null);
        Assert.assertNotNull(valSearch);
        Assert.assertEquals(valSearch, entry.getValue());
      }
      Assert.assertFalse(itr.hasNext());
    }

    Assert.assertTrue(keysSet.isEmpty());
    Assert.assertTrue(valuesSet.isEmpty());
  }

  // UTILITY

  private <K> void testReadKeyToString(K[] keys) {
    // Write
    String[] values = GenerateTestData.generateStringData(keys.length, 10);
    try (StoreWriter writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.putAll(keys, values);
    }
      // Read
    try (StoreReader<K,String> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.size(), keys.length);

      for (int i = 0; i < keys.length; i++) {
        K key = keys[i];
        String val = reader.get(key, null);
        Assert.assertNotNull(val);
        Assert.assertEquals(val, values[i]);
      }
    }
  }

  private <K> void testReadKeyToInt(K[] keys) {
    // Write
    Integer[] values = GenerateTestData.generateIntData(keys.length);
    try (StoreWriter writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.putAll(keys, values);
    }

    try (StoreReader<K, Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.size(), keys.length);

      for (int i = 0; i < keys.length; i++) {
        K key = keys[i];
        Object val = reader.get(key, 0);
        Assert.assertNotNull(val);
        Assert.assertEquals(val, values[i]);
      }
    }
  }

  private <K> void testReadKeyToNull(K[] keys) {
    //Write
    try (StoreWriter writer = PalDB.createWriter(storeFile, new Configuration())) {
      Object[] values = new Object[keys.length];
      writer.putAll(keys, values);
    }

    try (StoreReader<K, Object> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.size(), keys.length);

      for (K key : keys) {
        Object val = reader.get(key, null);
        Assert.assertNull(val);
      }

      for (K key : keys) {
        Object val = reader.get(key, null);
        Assert.assertNull(val);
      }
    }
  }

  private <K> void testReadKeyToIntArray(K[] keys) {
    //Write
    int[][] values = GenerateTestData.generateIntArrayData(keys.length, 100);
    try (StoreWriter writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.putAll(keys, values);
    }

    //Read
    try (StoreReader<K,int[]> reader = PalDB.createReader(storeFile, new Configuration())) {
      Assert.assertEquals(reader.size(), keys.length);

      for (int i = 0; i < keys.length; i++) {
        K key = keys[i];
        int[] val = reader.get(key, null);
        Assert.assertNotNull(val);
        Assert.assertEquals(val, values[i]);
      }
    }
  }

  private void writeStore(File location, Object[] keys, Object[] values) {
    try (StoreWriter writer = PalDB.createWriter(location, new Configuration())) {
      writer.putAll(keys, values);
    }
  }

  private void testKeyLength(Object key, int expectedLength) {
    StorageSerialization serializationImpl = new StorageSerialization(new Configuration());
    int keyLength = 0;
    try {
      keyLength = serializationImpl.serializeKey(key).length;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Assert.assertEquals(keyLength, expectedLength);
  }
}
