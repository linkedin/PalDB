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
import com.linkedin.paldb.api.errors.DuplicateKeyException;
import com.linkedin.paldb.performance.utils.DirectoryUtils;
import com.linkedin.paldb.utils.*;
import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import static com.linkedin.paldb.utils.TestTempUtils.deleteDirectory;
import static org.junit.jupiter.api.Assertions.*;

public class TestStore {

  private Path tempDir;
  private File storeFile;

  @BeforeEach
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("tmp");
    storeFile = Files.createTempFile(tempDir, "paldb", ".dat").toFile();
  }

  @AfterEach
  public void cleanUp() {
    deleteDirectory(tempDir.toFile());
  }

  @Test
  public void testEmpty() {
    StoreWriter writer = PalDB.createWriter(storeFile, new Configuration());
    writer.close();

    assertTrue(storeFile.exists());

    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.size(), 0);
      assertNull(reader.get(1, null));
    }
  }

  @Test
  public void testEmptyStream() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StoreWriter writer = PalDB.createWriter(bos, new Configuration());
    writer.close();

    assertTrue(bos.toByteArray().length > 0);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    StoreReader reader = PalDB.createReader(bis, new Configuration());
    reader.close();
  }

  @Test
  public void testEmptyDefaultConfig() {
    StoreWriter writer = PalDB.createWriter(storeFile);
    writer.close();

    assertTrue(storeFile.exists());

    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile)) {
      assertEquals(reader.size(), 0);
      assertNull(reader.get(1, null));
    }
  }

  @Test
  public void testNewConfiguration() {
    assertNotNull(PalDB.newConfiguration());
  }

  @Test
  public void testNoFolder() {
    File file = new File("nofolder.store");
    file.deleteOnExit();
    StoreWriter writer = PalDB.createWriter(file, new Configuration());
    writer.close();

    assertTrue(file.exists());
  }

  @Test
  public void testReaderFileNotFound() {
    assertThrows(RuntimeException.class, () -> PalDB.createReader(new File("notfound"), PalDB.newConfiguration()));
  }

  @Test
  public void testReaderNullFile() {
    assertThrows(NullPointerException.class, () -> PalDB.createReader((File) null, PalDB.newConfiguration()));
  }

  @Test
  public void testReaderNullConfig() {
    assertThrows(NullPointerException .class, () -> PalDB.createReader(new File("notfound"), null));
  }

  @Test
  public void testReaderNullStream() {
    assertThrows(NullPointerException.class, () -> PalDB.createReader((InputStream) null, PalDB.newConfiguration()));
  }

  @Test
  public void testReaderNullConfigForStream() {
    assertThrows(NullPointerException.class, () -> {
      PalDB.createReader(new InputStream() {
        @Override
        public int read() {
          return 0;
        }
      }, null);
    });
  }

  @Test
  public void testWriterNullFile() {
    assertThrows(NullPointerException.class, () -> PalDB.createWriter((File) null, PalDB.newConfiguration()));
  }

  @Test
  public void testWriterNullConfig() {
    assertThrows(NullPointerException.class, () -> PalDB.createWriter(new File("notfound"), null));
  }

  @Test
  public void testWriterNullStream() {
    assertThrows(NullPointerException.class, () -> PalDB.createWriter((OutputStream) null, PalDB.newConfiguration()));
  }

  @Test
  public void testWriterNullConfigForStream() {
    assertThrows(NullPointerException.class, () -> PalDB.createWriter(new OutputStream() {
      @Override
      public void write(int i) {

      }
    }, null));
  }

  @Test
  public void testInvalidSegmentSize() {
    StoreWriter writer = PalDB.createWriter(storeFile);
    writer.close();

    Configuration config = new Configuration();
    config.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(1 + (long) Integer.MAX_VALUE));
    assertThrows(IllegalArgumentException.class, () -> PalDB.createReader(storeFile, config));
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
      assertEquals(reader.size(), 0);
      assertNull(reader.get(1, null));
    }
  }

  @Test
  public void testOneKey() {
    try (StoreWriter<Integer,String> writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.put(1, "foo");
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.size(), 1);
      assertEquals(reader.get(1), "foo");
    }
  }

  @Test
  public void testPutSerializedKey() throws IOException {
    StorageSerialization storageSerialization = new StorageSerialization(new Configuration());
    byte[] serializedKey = storageSerialization.serializeKey(1);
    byte[] serializedValue = storageSerialization.serializeValue("foo");

    try (StoreWriter<Integer,String> writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.put(serializedKey, serializedValue);
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.size(), 1);
      assertEquals(reader.get(1), "foo");
    }
  }

  @Test
  public void testByteMarkOneKey() throws IOException {
    try (FileOutputStream fos = new FileOutputStream(storeFile);
         StoreWriter<Integer,String> writer = PalDB.createWriter(fos, new Configuration())) {
      fos.write(12345);
      fos.write(FormatVersion.getPrefixBytes()[0]);
      fos.write(3456);

      writer.put(1, "foo");
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.size(), 1);
      assertEquals(reader.get(1), "foo");
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
    writeStore(storeFile, new Integer[]{key1, key2}, new Integer[]{1, 6});

    //Read
    try (StoreReader<Integer, Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.get(key1).intValue(), 1);
      assertEquals(reader.get(key2).intValue(), 6);
      assertNull(reader.get(0, null));
      assertNull(reader.get(6, null));
      assertNull(reader.get(244, null));
      assertNull(reader.get(246, null));
      assertNull(reader.get(1245, null));
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
    writeStore(storeFile, new Integer[]{key1, key2}, new Integer[]{1, 6});

    //Read
    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.get(key1).intValue(), 1);
      assertEquals(reader.get(key2).intValue(), 6);
      assertNull(reader.get(0, null));
      assertNull(reader.get(6, null));
      assertNull(reader.get(244, null));
      assertNull(reader.get(267, null));
      assertNull(reader.get(2449, null));
      assertNull(reader.get(2451, null));
      assertNull(reader.get(2454441, null));
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
    writeStore(storeFile, new Integer[]{key1, key2}, new Integer[]{1, 6});

    //Read
    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.get(key1).intValue(), 1);
      assertEquals(reader.get(key2).intValue(), 6);
      assertNull(reader.get(6, null));
      assertNull(reader.get(244, null));
      assertNull(reader.get(267, null));
      assertNull(reader.get(2449, null));
      assertNull(reader.get(2451, null));
      assertNull(reader.get(2454441, null));
    }
  }

  @Test
  public void testDuplicateKeys() {
    StoreWriter<Integer,String> writer = PalDB.createWriter(storeFile, new Configuration());
    writer.put(0, "ABC");
    writer.put(0, "DGE");
    assertThrows(DuplicateKeyException.class, writer::close);
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
        assertEquals(reader.get((Integer) keys[i], null), values[i]);
      }
    }
  }

  @Test
  public void testDataSizeOnTwoBuffers() throws IOException {
    Integer[] keys = new Integer[]{1, 2, 3};
    String[] values = new String[]{GenerateTestData.generateStringData(100), GenerateTestData
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
        assertEquals(reader.get(keys[i], null), values[i]);
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
    String[] values = GenerateTestData.generateStringData(keys.length, 1000);
    try (StoreWriter<Integer,String> writer = PalDB.createWriter(storeFile, configuration)) {
      writer.putAll(keys, values);
    }

    //Read
    configuration.set(Configuration.MMAP_DATA_ENABLED, "false");
    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, configuration)) {
      assertEquals(reader.size(), keys.length);

      for (int i = 0; i < keys.length; i++) {
        Integer key = keys[i];
        String val = reader.get(key, null);
        assertNotNull(val);
        assertEquals(val, values[i]);
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
        assertTrue(itr.hasNext());
        var entry = itr.next();
        assertNotNull(entry);
        assertTrue(keysSet.remove(entry.getKey()));
        assertTrue(valuesSet.remove(entry.getValue()));

        Object valSearch = reader.get(entry.getKey(), null);
        assertNotNull(valSearch);
        assertEquals(valSearch, entry.getValue());
      }
      assertFalse(itr.hasNext());
    }

    assertTrue(keysSet.isEmpty());
    assertTrue(valuesSet.isEmpty());
  }

  @Test
  void should_read_file_close_file_and_delete_it() throws IOException {
    Integer[] keys = GenerateTestData.generateIntKeys(100);
    String[] values = GenerateTestData.generateStringData(keys.length, 12);

    //Write
    writeStore(storeFile, keys, values);

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertNotNull(reader.get(0));
    }

    Files.delete(storeFile.toPath());
    assertFalse(Files.exists(storeFile.toPath()));
    assertEquals(0L, DirectoryUtils.folderSize(tempDir.toFile()));
  }

  @Test
  void should_write_file_close_file_and_delete_it() throws IOException {
    Integer[] keys = GenerateTestData.generateIntKeys(100);
    String[] values = GenerateTestData.generateStringData(keys.length, 12);

    //Write
    writeStore(storeFile, keys, values);

    Files.delete(storeFile.toPath());
    assertFalse(Files.exists(storeFile.toPath()));
    assertEquals(0L, DirectoryUtils.folderSize(tempDir.toFile()));
  }

  // UTILITY

  private <K> void testReadKeyToString(K[] keys) {
    // Write
    String[] values = GenerateTestData.generateStringData(keys.length, 10);
    try (StoreWriter<K,String> writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.putAll(keys, values);
    }
      // Read
    try (StoreReader<K,String> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.size(), keys.length);

      for (int i = 0; i < keys.length; i++) {
        K key = keys[i];
        String val = reader.get(key, null);
        assertNotNull(val);
        assertEquals(val, values[i]);
      }
    }
  }

  private <K> void testReadKeyToInt(K[] keys) {
    // Write
    Integer[] values = GenerateTestData.generateIntData(keys.length);
    try (StoreWriter<K, Integer> writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.putAll(keys, values);
    }

    try (StoreReader<K, Integer> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.size(), keys.length);

      for (int i = 0; i < keys.length; i++) {
        K key = keys[i];
        Object val = reader.get(key, 0);
        assertNotNull(val);
        assertEquals(val, values[i]);
      }
    }
  }

  private <K> void testReadKeyToNull(K[] keys) {
    //Write
    try (StoreWriter<K, Object> writer = PalDB.createWriter(storeFile, new Configuration())) {
      Object[] values = new Object[keys.length];
      writer.putAll(keys, values);
    }

    try (StoreReader<K, Object> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.size(), keys.length);

      for (K key : keys) {
        Object val = reader.get(key, null);
        assertNull(val);
      }

      for (K key : keys) {
        Object val = reader.get(key, null);
        assertNull(val);
      }
    }
  }

  private <K> void testReadKeyToIntArray(K[] keys) {
    //Write
    int[][] values = GenerateTestData.generateIntArrayData(keys.length, 100);
    try (StoreWriter<K,int[]> writer = PalDB.createWriter(storeFile, new Configuration())) {
      writer.putAll(keys, values);
    }

    //Read
    try (StoreReader<K,int[]> reader = PalDB.createReader(storeFile, new Configuration())) {
      assertEquals(reader.size(), keys.length);

      for (int i = 0; i < keys.length; i++) {
        K key = keys[i];
        int[] val = reader.get(key, null);
        assertNotNull(val);
        assertArrayEquals(val, values[i]);
      }
    }
  }

  private <K,V> void writeStore(File location, K[] keys, V[] values) {
    try (StoreWriter<K,V> writer = PalDB.createWriter(location, new Configuration())) {
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
    assertEquals(keyLength, expectedLength);
  }
}
