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
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.utils.DataInputOutput;
import com.linkedin.paldb.utils.FormatVersion;
import com.linkedin.paldb.utils.LongPacker;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStore {

  private final File STORE_FOLDER = new File("data");
  private final File STORE_FILE = new File(STORE_FOLDER, "paldb.dat");

  @BeforeClass
  public void setUp() {
    STORE_FILE.delete();
    STORE_FOLDER.delete();
    STORE_FOLDER.mkdir();
  }

  @AfterClass
  public void cleanUp() {
    STORE_FILE.delete();
    STORE_FOLDER.delete();
  }

  @Test
  public void testEmpty() {
    StoreWriter writer = PalDB.createWriter(STORE_FILE, new Configuration());
    writer.close();

    Assert.assertTrue(STORE_FILE.exists());

    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());

    Assert.assertEquals(reader.size(), 0);
    Assert.assertNull(reader.get(1, null));

    reader.close();
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
    StoreWriter writer = PalDB.createWriter(STORE_FILE);
    writer.close();

    Assert.assertTrue(STORE_FILE.exists());

    StoreReader reader = PalDB.createReader(STORE_FILE);

    Assert.assertEquals(reader.size(), 0);
    Assert.assertNull(reader.get(1, null));

    reader.close();
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
    StoreWriter writer = PalDB.createWriter(STORE_FILE);
    writer.close();

    Configuration config = new Configuration();
    config.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(1 + (long) Integer.MAX_VALUE));
    PalDB.createReader(STORE_FILE, config);
  }

  @Test
  public void testByteMarkEmpty()
      throws IOException {
    FileOutputStream fos = new FileOutputStream(STORE_FILE);
    fos.write(12345);
    fos.write(FormatVersion.getPrefixBytes()[0]);
    fos.write(3456);
    StoreWriter writer = PalDB.createWriter(fos, new Configuration());
    writer.close();

    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());

    Assert.assertEquals(reader.size(), 0);
    Assert.assertNull(reader.get(1, null));

    reader.close();
  }

  @Test
  public void testOneKey() {
    StoreWriter writer = PalDB.createWriter(STORE_FILE, new Configuration());
    writer.put(1, "foo");
    writer.close();

    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());
    Assert.assertEquals(reader.size(), 1);
    Assert.assertEquals(reader.get(1), "foo");
    reader.close();
  }

  @Test
  public void testPutSerializedKey()
      throws IOException {
    StorageSerialization storageSerialization = new StorageSerialization(new Configuration());
    byte[] serializedKey = storageSerialization.serializeKey(1);
    byte[] serializedValue = storageSerialization.serializeValue("foo");

    StoreWriter writer = PalDB.createWriter(STORE_FILE, new Configuration());
    writer.put(serializedKey, serializedValue);
    writer.close();

    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());
    Assert.assertEquals(reader.size(), 1);
    Assert.assertEquals(reader.get(1), "foo");
    reader.close();
  }

  @Test
  public void testByteMarkOneKey()
      throws IOException {
    FileOutputStream fos = new FileOutputStream(STORE_FILE);
    fos.write(12345);
    fos.write(FormatVersion.getPrefixBytes()[0]);
    fos.write(3456);
    StoreWriter writer = PalDB.createWriter(fos, new Configuration());
    writer.put(1, "foo");
    writer.close();

    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());

    Assert.assertEquals(reader.size(), 1);
    Assert.assertEquals(reader.get(1), "foo");
    reader.close();
  }

  @Test
  public void testTwoFirstKeyLength()
      throws NotFoundException {
    Integer key1 = 1;
    Integer key2 = 245;

    //Test key length
    testKeyLength(key1, 1);
    testKeyLength(key2, 2);

    //Write
    writeStore(STORE_FILE, new Object[]{key1, key2}, new Object[]{1, 6});

    //Read
    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());
    Assert.assertEquals(reader.getInt(key1), 1);
    Assert.assertEquals(reader.getInt(key2), 6);
    Assert.assertNull(reader.get(0, null));
    Assert.assertNull(reader.get(6, null));
    Assert.assertNull(reader.get(244, null));
    Assert.assertNull(reader.get(246, null));
    Assert.assertNull(reader.get(1245, null));
  }

  @Test
  public void testKeyLengthGap()
      throws NotFoundException {
    Integer key1 = 1;
    Integer key2 = 2450;

    //Test key length
    testKeyLength(key1, 1);
    testKeyLength(key2, 3);

    //Write
    writeStore(STORE_FILE, new Object[]{key1, key2}, new Object[]{1, 6});

    //Read
    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());
    Assert.assertEquals(reader.getInt(key1), 1);
    Assert.assertEquals(reader.getInt(key2), 6);
    Assert.assertNull(reader.get(0, null));
    Assert.assertNull(reader.get(6, null));
    Assert.assertNull(reader.get(244, null));
    Assert.assertNull(reader.get(267, null));
    Assert.assertNull(reader.get(2449, null));
    Assert.assertNull(reader.get(2451, null));
    Assert.assertNull(reader.get(2454441, null));
  }

  @Test
  public void testKeyLengthStartTwo()
      throws NotFoundException {
    Integer key1 = 245;
    Integer key2 = 2450;

    //Test key length
    testKeyLength(key1, 2);
    testKeyLength(key2, 3);

    //Write
    writeStore(STORE_FILE, new Object[]{key1, key2}, new Object[]{1, 6});

    //Read
    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());
    Assert.assertEquals(reader.getInt(key1), 1);
    Assert.assertEquals(reader.getInt(key2), 6);
    Assert.assertNull(reader.get(6, null));
    Assert.assertNull(reader.get(244, null));
    Assert.assertNull(reader.get(267, null));
    Assert.assertNull(reader.get(2449, null));
    Assert.assertNull(reader.get(2451, null));
    Assert.assertNull(reader.get(2454441, null));
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*duplicate.*")
  public void testDuplicateKeys() {
    StoreWriter writer = PalDB.createWriter(STORE_FILE, new Configuration());
    writer.put(0, "ABC");
    writer.put(0, "DGE");
    writer.close();
  }

  @Test
  public void testDataOnTwoBuffers()
      throws IOException {
    Object[] keys = new Object[]{1, 2, 3};
    Object[] values = new Object[]{GenerateTestData.generateStringData(100), GenerateTestData
        .generateStringData(10000), GenerateTestData.generateStringData(100)};

    StorageSerialization serialization = new StorageSerialization(new Configuration());
    int byteSize = serialization.serialize(values[0]).length + serialization.serialize(values[1]).length;

    //Write
    writeStore(STORE_FILE, keys, values);

    //Read
    Configuration configuration = new Configuration();
    configuration.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(byteSize - 100));
    StoreReader reader = PalDB.createReader(STORE_FILE, configuration);
    for (int i = 0; i < keys.length; i++) {
      Assert.assertEquals(reader.get(keys[i], null), values[i]);
    }
  }

  @Test
  public void testDataSizeOnTwoBuffers()
      throws IOException {
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
    writeStore(STORE_FILE, keys, values);

    //Read
    Configuration configuration = new Configuration();
    configuration.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(byteSize + sizeSize + 3));
    StoreReader reader = PalDB.createReader(STORE_FILE, configuration);
    for (int i = 0; i < keys.length; i++) {
      Assert.assertEquals(reader.get(keys[i], null), values[i]);
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
    StoreWriter writer = PalDB.createWriter(STORE_FILE, configuration);
    Object[] values = GenerateTestData.generateStringData(keys.length, 1000);
    writer.putAll(keys, values);
    writer.close();

    //Read
    configuration.set(Configuration.MMAP_DATA_ENABLED, "false");
    StoreReader reader = PalDB.createReader(STORE_FILE, configuration);
    Assert.assertEquals(reader.size(), keys.length);

    for (int i = 0; i < keys.length; i++) {
      Object key = keys[i];
      Object val = reader.getString(key, null);
      Assert.assertNotNull(val);
      Assert.assertEquals(val, values[i]);
    }
    reader.close();
  }

  @Test
  public void testIterate() {
    Integer[] keys = GenerateTestData.generateIntKeys(100);
    String[] values = GenerateTestData.generateStringData(keys.length, 12);

    //Write
    writeStore(STORE_FILE, keys, values);

    //Sets
    Set<Integer> keysSet = new HashSet<Integer>(Arrays.asList(keys));
    Set<String> valuesSet = new HashSet<String>(Arrays.asList(values));

    //Read
    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());
    Iterator<Map.Entry<Object, Object>> itr = reader.iterable().iterator();
    for (int i = 0; i < keys.length; i++) {
      Assert.assertTrue(itr.hasNext());
      Map.Entry<Object, Object> entry = itr.next();
      Assert.assertNotNull(entry);
      Assert.assertTrue(keysSet.remove(entry.getKey()));
      Assert.assertTrue(valuesSet.remove(entry.getValue()));

      Object valSearch = reader.get(entry.getKey(), null);
      Assert.assertNotNull(valSearch);
      Assert.assertEquals(valSearch, entry.getValue());
    }
    Assert.assertFalse(itr.hasNext());
    reader.close();

    Assert.assertTrue(keysSet.isEmpty());
    Assert.assertTrue(valuesSet.isEmpty());
  }

  // UTILITY

  private void testReadKeyToString(Object[] keys) {
    // Write
    StoreWriter writer = PalDB.createWriter(STORE_FILE, new Configuration());
    Object[] values = GenerateTestData.generateStringData(keys.length, 10);
    writer.putAll(keys, values);
    writer.close();

    // Read
    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());
    Assert.assertEquals(reader.size(), keys.length);

    for (int i = 0; i < keys.length; i++) {
      Object key = keys[i];
      Object val = reader.getString(key, null);
      Assert.assertNotNull(val);
      Assert.assertEquals(val, values[i]);
    }

    reader.close();
  }

  private void testReadKeyToInt(Object[] keys) {
    // Write
    StoreWriter writer = PalDB.createWriter(STORE_FILE, new Configuration());
    Integer[] values = GenerateTestData.generateIntData(keys.length);
    writer.putAll(keys, values);
    writer.close();

    // Read
    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());
    Assert.assertEquals(reader.size(), keys.length);

    for (int i = 0; i < keys.length; i++) {
      Object key = keys[i];
      Object val = reader.getInt(key, 0);
      Assert.assertNotNull(val);
      Assert.assertEquals(val, values[i]);
    }

    reader.close();
  }

  private void testReadKeyToNull(Object[] keys) {
    //Write
    StoreWriter writer = PalDB.createWriter(STORE_FILE, new Configuration());
    Object[] values = new Object[keys.length];
    writer.putAll(keys, values);
    writer.close();

    //Read
    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());
    Assert.assertEquals(reader.size(), keys.length);

    for (int i = 0; i < keys.length; i++) {
      Object key = keys[i];
      Object val = reader.get(key, 0);
      Assert.assertNull(val);
    }
    for (int i = 0; i < keys.length; i++) {
      Object key = keys[i];
      Object val = reader.get(key, 0);
      Assert.assertNull(val);
    }

    reader.close();
  }

  private void testReadKeyToIntArray(Object[] keys) {
    //Write
    StoreWriter writer = PalDB.createWriter(STORE_FILE, new Configuration());
    int[][] values = GenerateTestData.generateIntArrayData(keys.length, 100);
    writer.putAll(keys, values);
    writer.close();

    //Read
    StoreReader reader = PalDB.createReader(STORE_FILE, new Configuration());
    Assert.assertEquals(reader.size(), keys.length);

    for (int i = 0; i < keys.length; i++) {
      Object key = keys[i];
      int[] val = reader.getIntArray(key, null);
      Assert.assertNotNull(val);
      Assert.assertEquals(val, values[i]);
    }

    reader.close();
  }

  private void writeStore(File location, Object[] keys, Object[] values) {
    StoreWriter writer = PalDB.createWriter(location, new Configuration());
    writer.putAll(keys, values);
    writer.close();
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
