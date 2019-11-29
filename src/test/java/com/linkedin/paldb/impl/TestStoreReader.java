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
import com.linkedin.paldb.api.errors.StoreClosed;
import org.junit.jupiter.api.*;

import java.awt.*;
import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.*;

import static com.linkedin.paldb.utils.FileUtils.deleteDirectory;
import static org.junit.jupiter.api.Assertions.*;

class TestStoreReader {

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

  @SafeVarargs
  private <V> StoreReader<Integer, V> readerForMany(V... values) {
    var configuration = new Configuration();
    configuration.registerSerializer(new PointSerializer());
    try (StoreWriter<Integer, V> writer = PalDB.createWriter(storeFile, configuration)) {
      for (int i = 0; i < values.length; i++) {
        writer.put(i, values[i]);
      }
    }
    return PalDB.createReader(storeFile, configuration);
  }

  private <V> StoreReader<Integer, V> readerFor(V value) {
    return readerForMany(value);
  }

  @Test
  void testFile() {
    try (var reader = readerFor(true)) {
      assertEquals(reader.getFile(), storeFile);
    }
  }

  @Test
  void testSize() {
    try (var reader = readerFor(true)) {
      assertEquals(1, reader.size());
    }
  }

  @Test
  void testStoreClosed() {
    var reader = readerFor(true);
    reader.close();
    assertThrows(StoreClosed.class, () -> reader.get(0));
  }

  @Test
  void testGetBoolean() {
    try (var reader = readerFor(true)) {
      assertTrue(reader.get(0));
      assertTrue(reader.get(0, false));
      assertFalse(reader.get(-1, false));
    }
  }

  @Test
  void testGetBooleanMissing() {
    try (var reader = readerFor(true)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetByte() {
    try (var reader = readerFor((byte)1)) {
      assertEquals(reader.get(0).byteValue(), (byte) 1);
      assertEquals(reader.get(0, (byte) 5).byteValue(), (byte) 1);
      assertEquals(reader.get(-1, (byte) 5).byteValue(), (byte) 5);
    }
  }

  @Test
  void testGetByteMissing() {
    try (var reader = readerFor((byte)1)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetChar() {
    try (var reader = readerFor('a')) {
      assertEquals('a', reader.get(0).charValue());
      assertEquals('a', reader.get(0, 'b').charValue());
      assertEquals('b', reader.get(-1, 'b').charValue());
    }
  }

  @Test
  void testGetCharMissing() {
    try (var reader = readerFor('a')) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetDouble() {
    try (var reader = readerFor(1.0)) {
      assertEquals(1.0, reader.get(0).doubleValue());
      assertEquals(1.0, reader.get(0, 2.0).doubleValue());
      assertEquals(2.0, reader.get(-1, 2.0).doubleValue());
    }
  }

  @Test
  void testGetDoubleMissing() {
    try (var reader = readerFor(1.0)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetFloat() {
    try (var reader = readerFor(1f)) {
      assertEquals(1f, reader.get(0).floatValue());
      assertEquals(1f, reader.get(0, 2f).floatValue());
      assertEquals(2f, reader.get(-1, 2f).floatValue());
    }
  }

  @Test
  void testGetFloatMissing() {
    try (var reader = readerFor(1.0)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetShort() {
    try (var reader = readerFor((short) 1)) {
      assertEquals((short) 1, reader.get(0).shortValue());
      assertEquals((short) 1, reader.get(0, (short) 2).shortValue());
      assertEquals((short) 2, reader.get(-1, (short) 2).shortValue());
    }
  }

  @Test
  void testGetShortMissing() {
    try (var reader = readerFor((short) 1)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetInt() {
    try (var reader = readerFor(1)) {
      assertEquals(1, reader.get(0).intValue());
      assertEquals(1, reader.get(0, 2).intValue());
      assertEquals(2, reader.get(-1, 2).intValue());
    }
  }

  @Test
  void testGetIntMissing() {
    try (var reader = readerFor(1)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetLong() {
    try (var reader = readerFor(1L)) {
      assertEquals(1L, reader.get(0).longValue());
      assertEquals(1L, reader.get(0, 2L).longValue());
      assertEquals(2L, reader.get(-1, 2L).longValue());
    }
  }

  @Test
  void testGetLongMissing() {
    try (var reader = readerFor(1L)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetString() {
    try (var reader = readerFor("foo")) {
      assertEquals("foo", reader.get(0));
      assertEquals("foo", reader.get(0, "bar"));
      assertEquals("bar", reader.get(-1, "bar"));
    }
  }

  @Test
  void testGetStringMissing() {
    try (var reader = readerFor("foo")) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetBooleanArray() {
    try (var reader = readerFor(new boolean[]{true})) {
      assertArrayEquals(new boolean[]{true}, reader.get(0));
      assertArrayEquals(new boolean[]{true}, reader.get(0, new boolean[]{false}));
      assertArrayEquals(new boolean[]{false}, reader.get(-1, new boolean[]{false}));
    }
  }

  @Test
  void testGetBooleanArrayMissing() {
    try (var reader = readerFor(new boolean[]{true})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetByteArray() {
    try (var reader = readerFor(new byte[]{1})) {
      assertArrayEquals(new byte[]{1}, reader.get(0));
      assertArrayEquals(new byte[]{1}, reader.get(0, new byte[]{2}));
      assertArrayEquals(new byte[]{2}, reader.get(-1, new byte[]{2}));
    }
  }

  @Test
  void testGetByteArrayMissing() {
    try (var reader = readerFor(new byte[]{1})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetCharArray() {
    try (var reader = readerFor(new char[]{'a'})) {
      assertArrayEquals(new char[]{'a'}, reader.get(0));
      assertArrayEquals(new char[]{'a'}, reader.get(0, new char[]{'b'}));
      assertArrayEquals(new char[]{'b'}, reader.get(-1, new char[]{'b'}));
    }
  }

  @Test
  void testGetCharArrayMissing() {
    try (var reader = readerFor(new char[]{'a'})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetDoubleArray() {
    try (var reader = readerFor(new double[]{1.0})) {
      assertArrayEquals(new double[]{1.0}, reader.get(0));
      assertArrayEquals(new double[]{1.0}, reader.get(0, new double[]{2.0}));
      assertArrayEquals(new double[]{2.0}, reader.get(-1, new double[]{2.0}));
    }
  }

  @Test
  void testGetDoubleArrayMissing() {
    try (var reader = readerFor(new double[]{1.0})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetFloatArray() {
    try (var reader = readerFor(new float[]{1f})) {
      assertArrayEquals(new float[]{1f}, reader.get(0));
      assertArrayEquals(new float[]{1f}, reader.get(0, new float[]{2f}));
      assertArrayEquals(new float[]{2f}, reader.get(-1, new float[]{2f}));
    }
  }

  @Test
  void testGetFloatArrayMissing() {
    try (var reader = readerFor(new float[]{1f})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetShortArray() {
    try (var reader = readerFor(new short[]{1})) {
      assertArrayEquals(new short[]{1}, reader.get(0));
      assertArrayEquals(new short[]{1}, reader.get(0, new short[]{2}));
      assertArrayEquals(new short[]{2}, reader.get(-1, new short[]{2}));
    }
  }

  @Test
  void testGetShortArrayMissing() {
    try (var reader = readerFor(new short[]{1})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetIntArray() {
    try (var reader = readerFor(new int[]{1})) {
      assertArrayEquals(new int[]{1}, reader.get(0));
      assertArrayEquals(new int[]{1}, reader.get(0, new int[]{2}));
      assertArrayEquals(new int[]{2}, reader.get(-1, new int[]{2}));
    }
  }

  @Test
  void testGetIntArrayMissing() {
    try (var reader = readerFor(new int[]{1})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetLongArray() {
    try (var reader = readerFor(new long[]{1L})) {
      assertArrayEquals(new long[]{1L}, reader.get(0));
      assertArrayEquals(new long[]{1L}, reader.get(0, new long[]{2L}));
      assertArrayEquals(new long[]{2L}, reader.get(-1, new long[]{2L}));
    }
  }

  @Test
  void testGetLongArrayMissing() {
    try (var reader = readerFor(new long[]{1L})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetStringArray() {
    try (var reader = readerFor(new String[]{"foo"})) {
      assertArrayEquals(new String[]{"foo"}, reader.get(0));
      assertArrayEquals(new String[]{"foo"}, reader.get(0, new String[]{"bar"}));
      assertArrayEquals(new String[]{"bar"}, reader.get(-1, new String[]{"bar"}));
    }
  }

  @Test
  void testGetStringArrayMissing() {
    try (var reader = readerFor(new String[]{"foo"})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetMissing() {
    try (var reader = readerFor("foo")) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetArray() {
    try (var reader = readerFor(new Object[]{"foo"})) {
      assertArrayEquals(new Object[]{"foo"}, reader.get(0));
      assertArrayEquals(new Object[]{"foo"}, reader.get(0, new Object[]{"bar"}));
      assertArrayEquals(new Object[]{"bar"}, reader.get(-1, new Object[]{"bar"}));
    }
  }

  @Test
  void testGetArrayMissing() {
    try (var reader = readerFor(new Object[]{"foo"})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  void testGetPoint() {
    try (var reader = readerFor(new Point(4, 56))) {
      assertEquals(new Point(4, 56), reader.get(0));
    }
  }

  @Test
  void testStream() {
    var values = List.of("foo", "bar");
    try (var reader = readerForMany(values.get(0), values.get(1));
        var stream = reader.stream()) {

      assertNotNull(stream);

      stream.forEach(v -> assertEquals(v.getValue(), values.get(v.getKey())));
    }
  }

  @Test
  void testStreamKeys() {
    var values = List.of("foo", "bar");
    try (var reader = readerForMany(values.get(0), values.get(1));
      var keys = reader.streamKeys()) {
      assertNotNull(keys);

      Set<Integer> actual = new HashSet<>();
      Set<Integer> expected = new HashSet<>();

        var ix = new AtomicInteger(0);

        keys.forEach(k -> {
          actual.add(k);
          expected.add(ix.getAndIncrement());
        });
      assertEquals(expected, actual);
    }
  }

  @Test
  void testMultiThreadRead() throws InterruptedException {
    int threadCount = 50;
    final CountDownLatch latch = new CountDownLatch(threadCount);
    final AtomicBoolean success = new AtomicBoolean(true);
    var values = List.of("foobar", "any", "any value");
    try (var reader = readerForMany(values.get(0), values.get(1))) {
      for(int i = 0; i < threadCount; i++) {
        new Thread(() -> {
          try {
            for(int c = 0; c < 100000; c++) {
              if(!success.get())break;
              assertEquals("any", reader.get(1));
              assertEquals("foobar", reader.get(0));
            }
          } catch (Throwable error){
            error.printStackTrace();
            success.set(false);
          } finally {
            latch.countDown();
          }
        }).start();
      }
      latch.await();
      assertTrue(success.get());
    }
  }

  // UTILITY

  public static class PointSerializer implements Serializer<Point> {

    @Override
    public Point read(DataInput input)
        throws IOException {
      return new Point(input.readInt(), input.readInt());
    }

    @Override
    public Class<Point> serializedClass() {
      return Point.class;
    }

    @Override
    public void write(DataOutput output, Point input)
        throws IOException {
      output.writeInt(input.x);
      output.writeInt(input.y);
    }
  }
}
