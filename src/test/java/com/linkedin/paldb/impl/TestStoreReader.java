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
import org.junit.jupiter.api.*;

import java.awt.*;
import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.linkedin.paldb.utils.TestTempUtils.deleteDirectory;
import static org.junit.jupiter.api.Assertions.*;

public class TestStoreReader {

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
  public void testFile() {
    try (var reader = readerFor(true)) {
      assertEquals(reader.getFile(), storeFile);
    }
  }

  @Test
  public void testSize() {
    try (var reader = readerFor(true)) {
      assertEquals(reader.size(), 1);
    }
  }

  @Test
  public void testStoreClosed() {
    var reader = readerFor(true);
    reader.close();
    assertThrows(IllegalStateException.class, () -> reader.get(0));
  }

  @Test
  public void testGetBoolean() {
    try (var reader = readerFor(true)) {
      assertTrue(reader.get(0));
      assertTrue(reader.get(0, false));
      assertFalse(reader.get(-1, false));
    }
  }

  @Test
  public void testGetBooleanMissing() {
    try (var reader = readerFor(true)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetByte() {
    try (var reader = readerFor((byte)1)) {
      assertEquals(reader.get(0).byteValue(), (byte) 1);
      assertEquals(reader.get(0, (byte) 5).byteValue(), (byte) 1);
      assertEquals(reader.get(-1, (byte) 5).byteValue(), (byte) 5);
    }
  }

  @Test
  public void testGetByteMissing() {
    try (var reader = readerFor((byte)1)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetChar() {
    try (var reader = readerFor('a')) {
      assertEquals(reader.get(0).charValue(), 'a');
      assertEquals(reader.get(0, 'b').charValue(), 'a');
      assertEquals(reader.get(-1, 'b').charValue(), 'b');
    }
  }

  @Test
  public void testGetCharMissing() {
    try (var reader = readerFor('a')) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetDouble() {
    try (var reader = readerFor(1.0)) {
      assertEquals(reader.get(0).doubleValue(), 1.0);
      assertEquals(reader.get(0, 2.0).doubleValue(), 1.0);
      assertEquals(reader.get(-1, 2.0).doubleValue(), 2.0);
    }
  }

  @Test
  public void testGetDoubleMissing() {
    try (var reader = readerFor(1.0)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetFloat() {
    try (var reader = readerFor(1f)) {
      assertEquals(reader.get(0).floatValue(), 1f);
      assertEquals(reader.get(0, 2f).floatValue(), 1f);
      assertEquals(reader.get(-1, 2f).floatValue(), 2f);
    }
  }

  @Test
  public void testGetFloatMissing() {
    try (var reader = readerFor(1.0)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetShort() {
    try (var reader = readerFor((short) 1)) {
      assertEquals(reader.get(0).shortValue(), (short) 1);
      assertEquals(reader.get(0, (short) 2).shortValue(), (short) 1);
      assertEquals(reader.get(-1, (short) 2).shortValue(), (short) 2);
    }
  }

  @Test
  public void testGetShortMissing() {
    try (var reader = readerFor((short) 1)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetInt() {
    try (var reader = readerFor(1)) {
      assertEquals(reader.get(0).intValue(), 1);
      assertEquals(reader.get(0, 2).intValue(), 1);
      assertEquals(reader.get(-1, 2).intValue(), 2);
    }
  }

  @Test
  public void testGetIntMissing() {
    try (var reader = readerFor(1)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetLong() {
    try (var reader = readerFor(1L)) {
      assertEquals(reader.get(0).longValue(), 1L);
      assertEquals(reader.get(0, 2L).longValue(), 1L);
      assertEquals(reader.get(-1, 2L).longValue(), 2L);
    }
  }

  @Test
  public void testGetLongMissing() {
    try (var reader = readerFor(1L)) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetString() {
    try (var reader = readerFor("foo")) {
      assertEquals(reader.get(0), "foo");
      assertEquals(reader.get(0, "bar"), "foo");
      assertEquals(reader.get(-1, "bar"), "bar");
    }
  }

  @Test
  public void testGetStringMissing() {
    try (var reader = readerFor("foo")) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetBooleanArray() {
    try (var reader = readerFor(new boolean[]{true})) {
      assertArrayEquals(reader.get(0), new boolean[]{true});
      assertArrayEquals(reader.get(0, new boolean[]{false}), new boolean[]{true});
      assertArrayEquals(reader.get(-1, new boolean[]{false}), new boolean[]{false});
    }
  }

  @Test
  public void testGetBooleanArrayMissing() {
    try (var reader = readerFor(new boolean[]{true})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetByteArray() {
    try (var reader = readerFor(new byte[]{1})) {
      assertArrayEquals(reader.get(0), new byte[]{1});
      assertArrayEquals(reader.get(0, new byte[]{2}), new byte[]{1});
      assertArrayEquals(reader.get(-1, new byte[]{2}), new byte[]{2});
    }
  }

  @Test
  public void testGetByteArrayMissing() {
    try (var reader = readerFor(new byte[]{1})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetCharArray() {
    try (var reader = readerFor(new char[]{'a'})) {
      assertArrayEquals(reader.get(0), new char[]{'a'});
      assertArrayEquals(reader.get(0, new char[]{'b'}), new char[]{'a'});
      assertArrayEquals(reader.get(-1, new char[]{'b'}), new char[]{'b'});
    }
  }

  @Test
  public void testGetCharArrayMissing() {
    try (var reader = readerFor(new char[]{'a'})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetDoubleArray() {
    try (var reader = readerFor(new double[]{1.0})) {
      assertArrayEquals(reader.get(0), new double[]{1.0});
      assertArrayEquals(reader.get(0, new double[]{2.0}), new double[]{1.0});
      assertArrayEquals(reader.get(-1, new double[]{2.0}), new double[]{2.0});
    }
  }

  @Test
  public void testGetDoubleArrayMissing() {
    try (var reader = readerFor(new double[]{1.0})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetFloatArray() {
    try (var reader = readerFor(new float[]{1f})) {
      assertArrayEquals(reader.get(0), new float[]{1f});
      assertArrayEquals(reader.get(0, new float[]{2f}), new float[]{1f});
      assertArrayEquals(reader.get(-1, new float[]{2f}), new float[]{2f});
    }
  }

  @Test
  public void testGetFloatArrayMissing() {
    try (var reader = readerFor(new float[]{1f})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetShortArray() {
    try (var reader = readerFor(new short[]{1})) {
      assertArrayEquals(reader.get(0), new short[]{1});
      assertArrayEquals(reader.get(0, new short[]{2}), new short[]{1});
      assertArrayEquals(reader.get(-1, new short[]{2}), new short[]{2});
    }
  }

  @Test
  public void testGetShortArrayMissing() {
    try (var reader = readerFor(new short[]{1})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetIntArray() {
    try (var reader = readerFor(new int[]{1})) {
      assertArrayEquals(reader.get(0), new int[]{1});
      assertArrayEquals(reader.get(0, new int[]{2}), new int[]{1});
      assertArrayEquals(reader.get(-1, new int[]{2}), new int[]{2});
    }
  }

  @Test
  public void testGetIntArrayMissing() {
    try (var reader = readerFor(new int[]{1})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetLongArray() {
    try (var reader = readerFor(new long[]{1L})) {
      assertArrayEquals(reader.get(0), new long[]{1L});
      assertArrayEquals(reader.get(0, new long[]{2L}), new long[]{1L});
      assertArrayEquals(reader.get(-1, new long[]{2L}), new long[]{2L});
    }
  }

  @Test
  public void testGetLongArrayMissing() {
    try (var reader = readerFor(new long[]{1L})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetStringArray() {
    try (var reader = readerFor(new String[]{"foo"})) {
      assertArrayEquals(reader.get(0), new String[]{"foo"});
      assertArrayEquals(reader.get(0, new String[]{"bar"}), new String[]{"foo"});
      assertArrayEquals(reader.get(-1, new String[]{"bar"}), new String[]{"bar"});
    }
  }

  @Test
  public void testGetStringArrayMissing() {
    try (var reader = readerFor(new String[]{"foo"})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetMissing() {
    try (var reader = readerFor("foo")) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetArray() {
    try (var reader = readerFor(new Object[]{"foo"})) {
      assertArrayEquals(reader.get(0), new Object[]{"foo"});
      assertArrayEquals(reader.get(0, new Object[]{"bar"}), new Object[]{"foo"});
      assertArrayEquals(reader.get(-1, new Object[]{"bar"}), new Object[]{"bar"});
    }
  }

  @Test
  public void testGetArrayMissing() {
    try (var reader = readerFor(new Object[]{"foo"})) {
      assertNull(reader.get(-1));
    }
  }

  @Test
  public void testGetPoint() {
    try (var reader = readerFor(new Point(4, 56))) {
      assertEquals(reader.get(0), new Point(4, 56));
    }
  }

  @Test
  public void testIterator() {
    var values = List.of("foo", "bar");
    try (var reader = readerForMany(values.get(0), values.get(1))) {
      var iter = reader.iterable();
      assertNotNull(iter);
      var itr = iter.iterator();
      assertNotNull(itr);

      for (int i = 0; i < values.size(); i++) {
        assertTrue(itr.hasNext());
        var v = itr.next();
        assertEquals(v.getValue(), values.get(v.getKey()));
      }
    }
  }

  @Test
  public void testIterate() {
    var values = List.of("foo", "bar");
    try (var reader = readerForMany(values.get(0), values.get(1))) {
      for (var entry: reader) {
        var val = values.get(entry.getKey());
        assertEquals(entry.getValue(), val);
      }
    }
  }

  @Test
  public void testKeyIterator() {
    var values = List.of("foo", "bar");
    try (var reader = readerForMany(values.get(0), values.get(1))) {
      var iter = reader.keys();
      assertNotNull(iter);
      var itr = iter.iterator();
      assertNotNull(itr);

      Set<Integer> actual = new HashSet<>();
      Set<Integer> expected = new HashSet<>();
      for (int i = 0; i < values.size(); i++) {
        assertTrue(itr.hasNext());
        Integer k = itr.next();
        actual.add(k);
        expected.add(i);
      }
      assertEquals(actual, expected);
    }
  }

  @Test
  public void testMultiThreadRead() throws InterruptedException {
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
              assertEquals(reader.get(1), "any");
              assertEquals(reader.get(0), "foobar");
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
