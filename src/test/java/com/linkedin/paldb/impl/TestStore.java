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

import static com.linkedin.paldb.utils.FileUtils.deleteDirectory;
import static org.junit.jupiter.api.Assertions.*;

class TestStore {

  private Path tempDir;
  private File storeFile;

  @BeforeEach
  void setUp() throws IOException {
    tempDir = Files.createTempDirectory("tmp");
    storeFile = Files.createTempFile(tempDir, "paldb", ".dat").toFile();
  }

  @AfterEach
  void cleanUp() {
    deleteDirectory(tempDir.toFile());
  }

  @Test
  void testEmpty() {
    var writer = PalDB.createWriter(storeFile, new Configuration<>());
    writer.close();

    assertTrue(storeFile.exists());

    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration<>())) {
      assertEquals(0, reader.size());
      assertNull(reader.get(1, null));
    }
  }

  @Test
  void testEmptyStream() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    var writer = PalDB.createWriter(bos, new Configuration<>());
    writer.close();

    assertTrue(bos.toByteArray().length > 0);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    var reader = PalDB.createReader(bis, new Configuration<>());
    reader.close();
  }

  @Test
  void testEmptyDefaultConfig() {
    var writer = PalDB.createWriter(storeFile);
    writer.close();

    assertTrue(storeFile.exists());

    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile)) {
      assertEquals(0, reader.size());
      assertNull(reader.get(1, null));
    }
  }

  @Test
  void testNewConfiguration() {
    assertNotNull(PalDB.newConfiguration());
  }

  @Test
  void testNoFolder() {
    File file = new File("nofolder.store");
    file.deleteOnExit();
    var writer = PalDB.createWriter(file, new Configuration<>());
    writer.close();

    assertTrue(file.exists());
  }

  @Test
  void testReaderFileNotFound() {
    assertThrows(RuntimeException.class, () -> PalDB.createReader(new File("notfound"), PalDB.newConfiguration()));
  }

  @Test
  void testReaderNullFile() {
    assertThrows(NullPointerException.class, () -> PalDB.createReader((File) null, PalDB.newConfiguration()));
  }

  @Test
  void testReaderNullConfig() {
    assertThrows(NullPointerException .class, () -> PalDB.createReader(new File("notfound"), null));
  }

  @Test
  void testReaderNullStream() {
    assertThrows(NullPointerException.class, () -> PalDB.createReader((InputStream) null, PalDB.newConfiguration()));
  }

  @Test
  void testReaderNullConfigForStream() {
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
  void testWriterNullFile() {
    assertThrows(NullPointerException.class, () -> PalDB.createWriter((File) null, PalDB.newConfiguration()));
  }

  @Test
  void testWriterNullConfig() {
    assertThrows(NullPointerException.class, () -> PalDB.createWriter(new File("notfound"), null));
  }

  @Test
  void testWriterNullStream() {
    assertThrows(NullPointerException.class, () -> PalDB.createWriter((OutputStream) null, PalDB.newConfiguration()));
  }

  @Test
  void testWriterNullConfigForStream() {
    assertThrows(NullPointerException.class, () -> PalDB.createWriter(new OutputStream() {
      @Override
      public void write(int i) {

      }
    }, null));
  }

  @Test
  void testInvalidSegmentSize() {
    var writer = PalDB.createWriter(storeFile);
    writer.close();

    var config = new Configuration<>();
    config.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(1 + (long) Integer.MAX_VALUE));
    assertThrows(IllegalArgumentException.class, () -> PalDB.createReader(storeFile, config));
  }

  @Test
  void testByteMarkEmpty() throws IOException {
    try (FileOutputStream fos = new FileOutputStream(storeFile)) {
      fos.write(12345);
      fos.write(FormatVersion.getPrefixBytes()[0]);
      fos.write(3456);
      var writer = PalDB.createWriter(fos, new Configuration<>());
      writer.close();
    }

    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration<>())) {
      assertEquals(0, reader.size());
      assertNull(reader.get(1, null));
    }
  }

  @Test
  void testOneKey() {
    try (StoreWriter<Integer,String> writer = PalDB.createWriter(storeFile, new Configuration<>())) {
      writer.put(1, "foo");
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration<>())) {
      assertEquals(1, reader.size());
      assertEquals("foo", reader.get(1));
    }
  }

  @Test
  void testPutSerializedKey() throws IOException {
    var storageSerialization = new StorageSerialization<>(new Configuration<>());
    byte[] serializedKey = storageSerialization.serializeKey(1);
    byte[] serializedValue = storageSerialization.serializeValue("foo");

    try (StoreWriter<Integer,String> writer = PalDB.createWriter(storeFile, new Configuration<>())) {
      writer.put(serializedKey, serializedValue);
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration<>())) {
      assertEquals(1, reader.size());
      assertEquals("foo", reader.get(1));
    }
  }

  @Test
  void testPutNullValue() {
    try (StoreWriter<Integer,String> writer = PalDB.createWriter(storeFile)) {
      writer.put(1, null);
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile)) {
      assertEquals(1, reader.size());
      assertNull(reader.get(1));
    }
  }

  @Test
  void testRemoveAfterPut() {
    try (StoreWriter<Integer,String> writer = PalDB.createWriter(storeFile)) {
      writer.put(1, "foo");
      writer.remove(1);
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile)) {
      assertEquals(0, reader.size());
      assertNull(reader.get(1));
    }
  }

  @Test
  void testRemoveArrayAfterPut() {
    try (StoreWriter<Integer,String[]> writer = PalDB.createWriter(storeFile)) {
      writer.put(1, new String[] {"foo"});
      writer.remove(1);
    }

    try (StoreReader<Integer,String[]> reader = PalDB.createReader(storeFile)) {
      assertEquals(0, reader.size());
      assertNull(reader.get(1));
    }
  }

  @Test
  void testRemoveBeforePut() {
    try (StoreWriter<Integer,String> writer = PalDB.createWriter(storeFile)) {
      writer.remove(1);
      writer.put(1, "foo");
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile)) {
      assertEquals(1, reader.size());
      assertEquals("foo", reader.get(1));
    }
  }

  @Test
  void testRemoveArrayBeforePut() {
    try (StoreWriter<Integer,String[]> writer = PalDB.createWriter(storeFile)) {
      writer.remove(1);
      writer.put(1, new String[] {"foo"});
    }

    try (StoreReader<Integer,String[]> reader = PalDB.createReader(storeFile)) {
      assertEquals(1, reader.size());
      assertArrayEquals(new String[] {"foo"}, reader.get(1));
    }
  }

  @Test
  void testByteMarkOneKey() throws IOException {
    try (FileOutputStream fos = new FileOutputStream(storeFile);
         StoreWriter<Integer,String> writer = PalDB.createWriter(fos, new Configuration<>())) {
      fos.write(12345);
      fos.write(FormatVersion.getPrefixBytes()[0]);
      fos.write(3456);

      writer.put(1, "foo");
    }

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, new Configuration<>())) {
      assertEquals(1, reader.size());
      assertEquals("foo", reader.get(1));
    }
  }

  @Test
  void testTwoFirstKeyLength() {
    Integer key1 = 1;
    Integer key2 = 245;

    //Test key length
    testKeyLength(key1, 1);
    testKeyLength(key2, 2);

    //Write
    writeStore(storeFile, new Integer[]{key1, key2}, new Integer[]{1, 6});

    //Read
    try (StoreReader<Integer, Integer> reader = PalDB.createReader(storeFile, new Configuration<>())) {
      assertEquals(1, reader.get(key1).intValue());
      assertEquals(6, reader.get(key2).intValue());
      assertNull(reader.get(0, null));
      assertNull(reader.get(6, null));
      assertNull(reader.get(244, null));
      assertNull(reader.get(246, null));
      assertNull(reader.get(1245, null));
    }
  }

  @Test
  void testKeyLengthGap() {
    Integer key1 = 1;
    Integer key2 = 2450;

    //Test key length
    testKeyLength(key1, 1);
    testKeyLength(key2, 3);

    //Write
    writeStore(storeFile, new Integer[]{key1, key2}, new Integer[]{1, 6});

    //Read
    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration<>())) {
      assertEquals(1, reader.get(key1).intValue());
      assertEquals(6, reader.get(key2).intValue());
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
  void testKeyLengthStartTwo() {
    Integer key1 = 245;
    Integer key2 = 2450;

    //Test key length
    testKeyLength(key1, 2);
    testKeyLength(key2, 3);

    //Write
    writeStore(storeFile, new Integer[]{key1, key2}, new Integer[]{1, 6});

    //Read
    try (StoreReader<Integer,Integer> reader = PalDB.createReader(storeFile, new Configuration<>())) {
      assertEquals(1, reader.get(key1).intValue());
      assertEquals(6, reader.get(key2).intValue());
      assertNull(reader.get(6, null));
      assertNull(reader.get(244, null));
      assertNull(reader.get(267, null));
      assertNull(reader.get(2449, null));
      assertNull(reader.get(2451, null));
      assertNull(reader.get(2454441, null));
    }
  }

  @Test
  void testDuplicateKeys() {
    StoreWriter<Integer,String> writer = PalDB.createWriter(storeFile, new Configuration<>());
    writer.put(0, "ABC");
    writer.put(0, "DGE");
    assertThrows(DuplicateKeyException.class, writer::close);
  }

  @Test
  void testDataOnTwoBuffers() throws IOException {
    Integer[] keys = new Integer[]{1, 2, 3};
    String[] values = new String[]{GenerateTestData.generateStringData(100), GenerateTestData
        .generateStringData(10000), GenerateTestData.generateStringData(100)};

    var serialization = new StorageSerialization<>(new Configuration<>());
    int byteSize = serialization.serializeValue(values[0]).length + serialization.serializeValue(values[1]).length;

    var configuration = new Configuration<Integer,String>();
    configuration.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(byteSize - 100));
    //Write
    writeStore(storeFile, keys, values, configuration);

    //Read
    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, configuration)) {
      for (int i = 0; i < keys.length; i++) {
        assertEquals(reader.get((Integer) keys[i], null), values[i]);
      }
    }
  }

  @Test
  void testIndexOnManyIndexBuffers() {
    var configuration = new Configuration<String, Integer>();
    configuration.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(32));

    var keys = new String[]{GenerateTestData.generateStringData(100), GenerateTestData
            .generateStringData(10000), GenerateTestData.generateStringData(100)};
    var values = new Integer[]{1, 2, 3};

    //Write
    writeStore(storeFile, keys, values, configuration);

    //Read
    try (StoreReader<String, Integer> reader = PalDB.createReader(storeFile, configuration)) {
      for (int i = 0; i < keys.length; i++) {
        assertEquals(reader.get(keys[i], null), values[i]);
      }
    }
  }

  @Test
  void testDataSizeOnTwoBuffers() throws IOException {
    Integer[] keys = new Integer[]{1, 2, 3};
    String[] values = new String[]{GenerateTestData.generateStringData(100), GenerateTestData
            .generateStringData(10000), GenerateTestData.generateStringData(100)};

    var serialization = new StorageSerialization<>(new Configuration<>());
    byte[] b1 = serialization.serializeValue(values[0]);
    byte[] b2 = serialization.serializeValue(values[1]);
    int byteSize = b1.length + b2.length;
    int sizeSize =
            LongPacker.packInt(new DataInputOutput(), b1.length) + LongPacker.packInt(new DataInputOutput(), b2.length);
    var configuration = new Configuration<Integer,String>();
    configuration.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(byteSize + sizeSize + 3));

    //Write
    writeStore(storeFile, keys, values, configuration);
    //Read
    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile, configuration)) {
      for (int i = 0; i < keys.length; i++) {
        assertEquals(reader.get(keys[i], null), values[i]);
      }
    }
  }

  @Test
  void testReadStringToString() {
    testReadKeyToString(GenerateTestData.generateStringKeys(100));
  }

  @Test
  void testReadIntToString() {
    testReadKeyToString(GenerateTestData.generateIntKeys(100));
  }

  @Test
  void testReadDoubleToString() {
    testReadKeyToString(GenerateTestData.generateDoubleKeys(100));
  }

  @Test
  void testReadLongToString() {
    testReadKeyToString(GenerateTestData.generateLongKeys(100));
  }

  @Test
  void testReadStringToInt() {
    testReadKeyToInt(GenerateTestData.generateStringKeys(100));
  }

  @Test
  void testReadByteToInt() {
    testReadKeyToInt(GenerateTestData.generateByteKeys(100));
  }

  @Test
  void testReadIntToInt() {
    testReadKeyToInt(GenerateTestData.generateIntKeys(100));
  }

  @Test
  void testReadIntToIntArray() {
    testReadKeyToIntArray(GenerateTestData.generateIntKeys(100));
  }

  @Test
  void testReadCompoundToString() {
    testReadKeyToString(GenerateTestData.generateCompoundKeys(100));
  }

  @Test
  void testReadCompoundByteToString() {
    testReadKeyToString(new Object[]{GenerateTestData.generateCompoundByteKey()});
  }

  @Test
  void testReadIntToNull() {
    testReadKeyToNull(GenerateTestData.generateIntKeys(100));
  }

  @Test
  void testReadDisk() {
    Integer[] keys = GenerateTestData.generateIntKeys(10000);
    var configuration = new Configuration<Integer,String>();

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
  void testIterate() {
    Integer[] keys = GenerateTestData.generateIntKeys(100);
    String[] values = GenerateTestData.generateStringData(keys.length, 12);

    //Write
    writeStore(storeFile, keys, values);

    //Sets
    Set<Integer> keysSet = new HashSet<>(Arrays.asList(keys));
    Set<String> valuesSet = new HashSet<>(Arrays.asList(values));

    //Read
    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile);
      var stream = reader.stream()) {

      stream.forEach(entry -> {
        assertNotNull(entry);
        assertTrue(keysSet.remove(entry.getKey()));
        assertTrue(valuesSet.remove(entry.getValue()));

        Object valSearch = reader.get(entry.getKey(), null);
        assertNotNull(valSearch);
        assertEquals(valSearch, entry.getValue());
      });
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

    try (StoreReader<Integer,String> reader = PalDB.createReader(storeFile)) {
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

  @Test
  void should_allow_duplicates() {
    var config = new Configuration<String,byte[]>();
    config.set(Configuration.ALLOW_DUPLICATES, String.valueOf(true));

    try (StoreWriter<String,byte[]> writer = PalDB.createWriter(storeFile, config)) {
      writer.put("foobar", EMPTY_VALUE);
      writer.put("foobar", "test".getBytes());
      writer.put("any data", "test2".getBytes());
    }

    try (StoreReader<String,byte[]> reader = PalDB.createReader(storeFile, config)) {
      assertArrayEquals("test".getBytes(), reader.get("foobar"));
      assertArrayEquals("test2".getBytes(), reader.get("any data"));
      assertEquals(2, reader.size());
    }
  }

  @Test
  void should_not_allow_put_null_keys() {
    try (StoreWriter<String,byte[]> writer = PalDB.createWriter(storeFile)) {
      assertThrows(NullPointerException.class, () -> writer.put((String)null, EMPTY_VALUE));
      assertThrows(NullPointerException.class, () -> writer.putAll((String[])null, new byte[][]{}));
    }
  }

  @Test
  void should_not_allow_getting_null_keys() {
    try (StoreWriter<String,byte[]> writer = PalDB.createWriter(storeFile)) {
      writer.put("any value", EMPTY_VALUE);
    }

    try (StoreReader<String,byte[]> reader = PalDB.createReader(storeFile)) {
      assertThrows(NullPointerException.class, () -> reader.get(null));
    }
  }

  @Test
  void should_not_find_when_bloom_filter_enabled() {
    var config = PalDBConfigBuilder.<String,byte[]>create()
            .withEnableBloomFilter(true).build();
    try (StoreWriter<String,byte[]> writer = PalDB.createWriter(storeFile, config)) {
      writer.put("abc", EMPTY_VALUE);
    }

    try (StoreReader<String,byte[]> reader = PalDB.createReader(storeFile, config)) {
      assertNull(reader.get("foo"));
      assertArrayEquals(EMPTY_VALUE, reader.get("abc"));
    }
  }

  private static final byte[] EMPTY_VALUE = new byte[0];

  @Test
  @Disabled
  void should_add_more_than_int_max_size_keys() {
    long keysCount = (long) Integer.MAX_VALUE + 100L;
    var from = System.currentTimeMillis();
    try (StoreWriter<Long,byte[]> writer = PalDB.createWriter(storeFile)) {
      System.out.println(String.format("Adding %d keys...", keysCount));
      for (long i = 0; i < keysCount; i++) {
        writer.put(i, EMPTY_VALUE);
      }
    }
    System.out.println(String.format("%d keys were added successfully in %d ms", keysCount, System.currentTimeMillis() - from));

    from = System.currentTimeMillis();
    try (StoreReader<Long,byte[]> reader = PalDB.createReader(storeFile)) {
      for (long i = 0; i < keysCount; i++) {
        assertArrayEquals(EMPTY_VALUE, reader.get(i));
      }
    }
    System.out.println(String.format("%d keys were read in %d ms", keysCount, System.currentTimeMillis() - from));
  }

  // UTILITY

  private <K> void testReadKeyToString(K[] keys) {
    // Write
    String[] values = GenerateTestData.generateStringData(keys.length, 10);
    try (StoreWriter<K,String> writer = PalDB.createWriter(storeFile)) {
      writer.putAll(keys, values);
    }
      // Read
    try (StoreReader<K,String> reader = PalDB.createReader(storeFile)) {
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
    try (StoreWriter<K, Object> writer = PalDB.createWriter(storeFile)) {
      Object[] values = new Object[keys.length];
      writer.putAll(keys, values);
    }

    try (StoreReader<K, Object> reader = PalDB.createReader(storeFile)) {
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
    try (StoreWriter<K,int[]> writer = PalDB.createWriter(storeFile)) {
      writer.putAll(keys, values);
    }

    //Read
    try (StoreReader<K,int[]> reader = PalDB.createReader(storeFile)) {
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
    writeStore(location, keys, values, new Configuration<>());
  }

  private <K,V> void writeStore(File location, K[] keys, V[] values, Configuration<K,V> configuration) {
    try (StoreWriter<K,V> writer = PalDB.createWriter(location, configuration)) {
      writer.putAll(keys, values);
    }
  }

  private void testKeyLength(Object key, int expectedLength) {
    var serializationImpl = new StorageSerialization<>(new Configuration<>());
    int keyLength;
    try {
      keyLength = serializationImpl.serializeKey(key).length;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    assertEquals(expectedLength, keyLength);
  }
}
