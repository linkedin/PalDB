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
import com.linkedin.paldb.api.errors.*;
import org.junit.jupiter.api.*;

import java.awt.*;
import java.io.*;
import java.lang.invoke.*;
import java.math.*;
import java.nio.*;

import static org.junit.jupiter.api.Assertions.*;

class TestStorageSerialization {

  private StorageSerialization<Integer,Integer> serialization;

  @BeforeEach
  void setUp() {
    serialization = new StorageSerialization<>(new Configuration<>());
  }

  @Test
  void testCompressionEnabled() {
    assertFalse(serialization.isCompressionEnabled());
    var config = new Configuration<>();
    config.set(Configuration.COMPRESSION_ENABLED, "true");
    var s = new StorageSerialization<>(config);
    assertTrue(s.isCompressionEnabled());
  }

  @Test
  void testSerializeKey() throws IOException {
    Integer l = 1;
    Object d = serialization.deserializeKey(serialization.serializeKey(l));
    assertEquals(l, d);
  }

  @Test
  void testSerializeKeyDataOutput() throws IOException {
    Integer l = 1;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    serialization.serializeKey(l, dos);
    dos.close();
    bos.close();

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream dis = new DataInputStream(bis);
    assertEquals(l, serialization.deserializeKey(dis));
  }

  @Test
  void testSerializeValueDataOutput() throws IOException {
    Integer l = 1;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    serialization.serializeValue(l, dos);
    dos.close();
    bos.close();

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream dis = new DataInputStream(bis);
    assertEquals(l, serialization.deserializeValue(dis));
  }

  @Test
  void testSerializeKeyNull() {
    assertThrows(NullPointerException.class, () -> serialization.serializeKey(null));
  }

  @Test
  void testTransformValue() throws IOException {
    Integer l = 1;
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertEquals(l, deserialize);
  }

  @Test
  void testTransformList() throws IOException {
    var l = new int[]{1, 2};
    var serialization = new StorageSerialization<Integer, int[]>(new Configuration<>());
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertEquals(int[].class, deserialize.getClass());
    assertArrayEquals(new int[]{1, 2}, deserialize);
  }

  @Test
  void testTransformListWith0() throws IOException {
    var l = new int[]{1, 0, 2};
    var serialization = new StorageSerialization<Integer, int[]>(new Configuration<>());
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertEquals(int[].class, deserialize.getClass());
    assertArrayEquals(new int[]{1, 0, 2}, deserialize);
  }

  @Test
  void testTransformListOfList() throws IOException {
    var l = new int[][]{{1}, {2}};
    var serialization = new StorageSerialization<Integer, int[][]>(new Configuration<>());
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertEquals(int[][].class, deserialize.getClass());
    assertArrayEquals(new int[][]{{1}, {2}}, deserialize);
  }

  @Test
  void testCustomSerializer() throws Throwable {
    var configuration = new Configuration<Integer,Point>();
    var serialization = new StorageSerialization<>(configuration);
    configuration.registerValueSerializer(new TestStoreReader.PointSerializer());
    Point p = new Point(42, 9);
    byte[] buf = serialization.serializeValue(p);
    assertEquals(p, serialization.deserializeValue(buf));
  }

  @Test
  void testCustomArraySerializer() throws Throwable {
    var configuration = new Configuration<Integer,Point[]>();
    var serialization = new StorageSerialization<>(configuration);
    configuration.registerValueSerializer(new Serializer<>() {
      VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

      @Override
      public byte[] write(Point[] input) {
        var buffer = new byte[8];
        INT_HANDLE.set(buffer, 0, input[0].x);
        INT_HANDLE.set(buffer, 4, input[0].y);
        return buffer;
      }

      @Override
      public Point[] read(byte[] bytes) {
        int x = (int)INT_HANDLE.get(bytes, 0);
        int y = (int)INT_HANDLE.get(bytes, 4);
        return new Point[] { new Point(x,y)};
      }
    });
    Point[] p = new Point[]{new Point(42, 9)};
    byte[] buf = serialization.serializeValue(p);
    assertArrayEquals(p, serialization.deserializeValue(buf));
  }

  @Test
  void testInnerClassSerializer() throws Throwable {
    var configuration = new Configuration<Integer,ImplementsA>();
    var serialization = new StorageSerialization<>(configuration);
    configuration.registerValueSerializer(new Serializer<>() {
      @Override
      public byte[] write(ImplementsA input) {
        return ByteBuffer.allocate(4).putInt(input.getVal()).array();
      }

      @Override
      public ImplementsA read(byte[] bytes) {
        return new ImplementsA(ByteBuffer.wrap(bytes).getInt());
      }
    });
    ImplementsA a = new ImplementsA(42);
    byte[] buf = serialization.serializeValue(a);
    assertEquals(a, serialization.deserializeValue(buf));
  }

  @Test
  void testInheritedSerializer() throws IOException {
    var configuration = new Configuration<Integer,A>();
    var serialization = new StorageSerialization<>(configuration);
    configuration.registerValueSerializer(new Serializer<>() {

      @Override
      public byte[] write(A input) {
        return ByteBuffer.allocate(4).putInt(input.getVal()).array();
      }

      @Override
      public A read(byte[] bytes) {
        return new ImplementsA(ByteBuffer.wrap(bytes).getInt());
      }
    });

    var a = new ImplementsA(42);
    byte[] buf = serialization.serializeValue(a);
    assertEquals(a, serialization.deserializeValue(buf));
  }

  @Test
  void testNull() throws Throwable {
    byte[] buf = serialization.serializeValue(null);
    assertNull(serialization.deserializeValue(buf));
  }

  @Test
  void testByte() throws Throwable {
    var configuration = new Configuration<Byte,Integer>();
    var serialization = new StorageSerialization<>(configuration);
    byte[] vals = new byte[]{-1, 0, 1, 6};
    for (byte val : vals) {
      byte[] buf = serialization.serializeKey(val);
      Object l2 = serialization.deserializeKey(buf);
      assertSame(Byte.class, l2.getClass());
      assertEquals(val, l2);
    }
  }

  @Test
  void testNotSupported() {
    var configuration = new Configuration<Integer,Color>();
    var serialization = new StorageSerialization<>(configuration);
    assertThrows(MissingSerializer.class, () -> serialization.serializeValue(new Color(0, 0, 0)));
  }

  @Test
  void testInt() throws IOException {
    int[] vals = {Integer.MIN_VALUE,
        -Short.MIN_VALUE * 2, -Short.MIN_VALUE
        + 1, -Short.MIN_VALUE, -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 127, 254, 255, 256, Short.MAX_VALUE,
        Short.MAX_VALUE + 1, Short.MAX_VALUE * 2, Integer.MAX_VALUE};
    for (int i : vals) {
      byte[] buf = serialization.serializeKey(i);
      Object l2 = serialization.deserializeKey(buf);
      assertSame(Integer.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testShort() throws IOException {
    short[] vals = {(short) (-Short.MIN_VALUE
        + 1), (short) -Short.MIN_VALUE, -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 127, 254, 255, 256, Short.MAX_VALUE,
        Short.MAX_VALUE - 1, Short.MAX_VALUE};
    var serialization = new StorageSerialization<>(new Configuration<Short,Integer>());
    for (short i : vals) {
      byte[] buf = serialization.serializeKey(i);
      Object l2 = serialization.deserializeKey(buf);
      assertSame(Short.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testDouble() throws IOException {
    double[] vals = {1f, 0f, -1f, Math.PI, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1, -100};
    var serialization = new StorageSerialization<>(new Configuration<Double,Integer>());
    for (double i : vals) {
      byte[] buf = serialization.serializeKey(i);
      Object l2 = serialization.deserializeKey(buf);
      assertSame(Double.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testFloat() throws IOException {
    float[] vals = {1f, 0f, -1f, (float) Math.PI, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1, -100};
    var serialization = new StorageSerialization<>(new Configuration<Float,Integer>());
    for (float i : vals) {
      byte[] buf = serialization.serializeKey(i);
      Object l2 = serialization.deserializeKey(buf);
      assertSame(Float.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testChar() throws IOException {
    char[] vals = {'a', ' '};
    var serialization = new StorageSerialization<>(new Configuration<Character,Integer>());
    for (char i : vals) {
      byte[] buf = serialization.serializeKey(i);
      Object l2 = serialization.deserializeKey(buf);
      assertSame(Character.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testLong() throws IOException {
    long[] vals = {Long.MIN_VALUE, Integer.MIN_VALUE,
        Integer.MIN_VALUE - 1,
        Integer.MIN_VALUE + 1,
        -Short.MIN_VALUE * 2, -Short.MIN_VALUE
        + 1, -Short.MIN_VALUE, -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 127, 254, 255, 256, Short.MAX_VALUE,
        Short.MAX_VALUE + 1, Short.MAX_VALUE * 2, Integer.MAX_VALUE, Integer.MAX_VALUE + 1, Long.MAX_VALUE};
    var serialization = new StorageSerialization<>(new Configuration<Long,Integer>());
    for (long i : vals) {
      byte[] buf = serialization.serializeKey(i);
      Object l2 = serialization.deserializeKey(buf);
      assertSame(Long.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testBoolean() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<Short,Boolean>());
    byte[] buf = serialization.serializeValue(true);
    Object l2 = serialization.deserializeValue(buf);
    assertSame(Boolean.class, l2.getClass());
    assertEquals(true, l2);

    byte[] buf2 = serialization.serializeValue(false);
    Object l22 = serialization.deserializeValue(buf2);
    assertSame(Boolean.class, l22.getClass());
    assertEquals(false, l22);
  }

  @Test
  void testString() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<String,Integer>());
    byte[] buf = serialization.serializeKey("Abcd");
    String l2 = serialization.deserializeKey(buf);
    assertEquals("Abcd", l2);
  }

  @Test
  void testEmptyString() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<String,Integer>());
    byte[] buf = serialization.serializeKey("");
    String l2 = (String) serialization.deserializeKey(buf);
    assertEquals("", l2);
  }

  @Test
  void testBigString() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<String,Integer>());
    var bigString = new StringBuilder();
    for (int i = 0; i < 1e4; i++) {
      bigString.append(i % 10);
    }
    byte[] buf = serialization.serializeKey(bigString.toString());
    String l2 = serialization.deserializeKey(buf);
    assertEquals(bigString.toString(), l2);
  }

  @Test
  void testClass() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<Class<String>,Integer>());
    byte[] buf = serialization.serializeKey(String.class);
    var l2 = serialization.deserializeKey(buf);
    assertEquals(String.class, l2);
  }

  @Test
  void testClass2() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<Class<long[]>,Integer>());
    byte[] buf = serialization.serializeKey(long[].class);
    var l2 = serialization.deserializeKey(buf);
    assertEquals(long[].class, l2);
  }

  @Test
  void testUnicodeString() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<String,Integer>());
    String s = "Ciudad Bolíva";
    byte[] buf = serialization.serializeKey(s);
    var l2 = serialization.deserializeKey(buf);
    assertEquals(s, l2);
  }

  @Test
  void testStringArray() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<String[],Integer>());
    String[] l = new String[]{"foo", "bar", ""};
    var deserialize = serialization.deserializeKey(serialization.serializeKey(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testObjectArray() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<Object[],Integer>());
    Object[] l = new Object[]{"foo", 2, Boolean.TRUE};
    var deserialize = serialization.deserializeKey(serialization.serializeKey(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testBooleanArray() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<boolean[],Integer>());
    var l = new boolean[]{true, false};
    var deserialize = serialization.deserializeKey(serialization.serializeKey(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testDoubleArray() throws IOException {
    var l = new double[]{Math.PI, 1D};
    var serialization = new StorageSerialization<>(new Configuration<double[],Integer>());
    var deserialize = serialization.deserializeKey(serialization.serializeKey(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testFloatArray() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<float[],Integer>());
    var l = new float[]{1F, 1.234235F};
    var deserialize = serialization.deserializeKey(serialization.serializeKey(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testByteArray() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<byte[],Integer>());
    byte[] l = new byte[]{1, 34, -5};
    var deserialize = serialization.deserializeKey(serialization.serializeKey(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testShortArray() throws Exception {
    var serialization = new StorageSerialization<>(new Configuration<short[],Integer>());
    short[] l = new short[]{1, 345, -5000};
    var deserialize = serialization.deserializeKey(serialization.serializeKey(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testCharArray() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<char[],Integer>());
    char[] l = new char[]{'1', 'a', '&'};
    var deserialize = serialization.deserializeKey(serialization.serializeKey(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testIntArray() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<int[],Integer>());
    int[][] l = new int[][]{{3, 5}, {-1200, 29999}, {3, 100000}, {-43999, 100000}};
    for (int[] a : l) {
      var deserialize = serialization.deserializeKey(serialization.serializeKey(a));
      assertArrayEquals(a, deserialize);
    }
  }

  @Test
  void testLongArray() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<long[],Integer>());
    long[][] l = new long[][]{{3L, 5L}, {-1200L, 29999L}, {3L, 100000L}, {-43999L, 100000L}, {-123L, 12345678901234L}};
    for (long[] a : l) {
      var deserialize = serialization.deserializeKey(serialization.serializeKey(a));
      assertArrayEquals(a, deserialize);
    }
  }

  @Test
  void testDoubleCompressedArray() throws IOException {
    var configuration = new Configuration<Integer, double[]>();
    configuration.set(Configuration.COMPRESSION_ENABLED, "true");
    var serialization = new StorageSerialization<>(configuration);
    double[] l = generateDoubleArray(500);
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testFloatCompressedArray() throws IOException {
    var configuration = new Configuration<Integer, float[]>();
    configuration.set(Configuration.COMPRESSION_ENABLED, "true");
    var serialization = new StorageSerialization<>(configuration);
    float[] l = generateFloatArray(500);
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testByteCompressedArray() throws IOException {
    var configuration = new Configuration<Integer, byte[]>();
    configuration.set(Configuration.COMPRESSION_ENABLED, "true");
    var serialization = new StorageSerialization<>(configuration);
    byte[] l = generateByteArray(500);
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testCharCompressedArray() throws IOException {
    var configuration = new Configuration<Integer, char[]>();
    configuration.set(Configuration.COMPRESSION_ENABLED, "true");
    var serialization = new StorageSerialization<>(configuration);
    char[] l = generateCharArray(500);
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testShortCompressedArray() throws IOException {
    var configuration = new Configuration<Integer, short[]>();
    configuration.set(Configuration.COMPRESSION_ENABLED, "true");
    var serialization = new StorageSerialization<>(configuration);
    short[] l = generateShortArray(500);
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testIntCompressedArray() throws IOException {
    var configuration = new Configuration<Integer, int[]>();
    configuration.set(Configuration.COMPRESSION_ENABLED, "true");
    var serialization = new StorageSerialization<>(configuration);
    int[] l = generateIntArray(500);
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testLongCompressedArray() throws IOException {
    var configuration = new Configuration<Integer, long[]>();
    configuration.set(Configuration.COMPRESSION_ENABLED, "true");
    var serialization = new StorageSerialization<>(configuration);
    long[] l = generateLongArray(500);
    var deserialize = serialization.deserializeValue(serialization.serializeValue(l));
    assertArrayEquals(l, deserialize);
  }

  @Test
  void testBigDecimal() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<BigDecimal,Integer>());
    BigDecimal d = new BigDecimal("445656.7889889895165654423236");
    assertEquals(d, serialization.deserializeKey(serialization.serializeKey(d)));
    d = new BigDecimal("-53534534534534445656.7889889895165654423236");
    assertEquals(d, serialization.deserializeKey(serialization.serializeKey(d)));
  }

  @Test
  void testBigInteger() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<BigInteger,Integer>());
    BigInteger d = new BigInteger("4456567889889895165654423236");
    assertEquals(d, serialization.deserializeKey(serialization.serializeKey(d)));
    d = new BigInteger("-535345345345344456567889889895165654423236");
    assertEquals(d, serialization.deserializeKey(serialization.serializeKey(d)));
  }

  @Test
  void testMultiDimensionalIntArray() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<int[][],Integer>());
    int[][] d = new int[2][];
    d[0] = new int[]{1, 3};
    d[1] = new int[]{-3, 1};
    var res = serialization.deserializeKey(serialization.serializeKey(d));
    assertEquals(res.getClass(), int[][].class);
    assertArrayEquals(d, res);
  }

  @Test
  void testMultiDimensionalLongArray() throws IOException {
    var serialization = new StorageSerialization<>(new Configuration<long[][],Integer>());
    long[][] d = new long[2][];
    d[0] = new long[]{1, 3};
    d[1] = new long[]{-3, 1};
    var res = serialization.deserializeKey(serialization.serializeKey(d));
    assertEquals(res.getClass(), long[][].class);
    assertArrayEquals(d, res);
  }

  // UTILITY

  private static int[] generateIntArray(int size) {
    int[] array = new int[size];
    double range = size * size;
    for (int i = 0; i < array.length; i++) {
      array[i] = (int) (Math.random() * range - range / 2.0);
    }
    return array;
  }

  private static long[] generateLongArray(int size) {
    long[] array = new long[size];
    double range = size * size;
    for (int i = 0; i < array.length; i++) {
      array[i] = (int) (Math.random() * range - range / 2.0);
    }
    return array;
  }

  private static short[] generateShortArray(int size) {
    short[] array = new short[size];
    double range = Short.MAX_VALUE;
    for (int i = 0; i < array.length; i++) {
      array[i] = (short) (Math.random() * range - range / 2.0);
    }
    return array;
  }

  private static double[] generateDoubleArray(int size) {
    double[] array = new double[size];
    double range = size * size;
    for (int i = 0; i < array.length; i++) {
      array[i] = (double) (Math.random() * range - range / 2.0);
    }
    return array;
  }

  private static float[] generateFloatArray(int size) {
    float[] array = new float[size];
    double range = Float.MAX_VALUE;
    for (int i = 0; i < array.length; i++) {
      array[i] = (float) (Math.random() * range - range / 2.0);
    }
    return array;
  }

  private static byte[] generateByteArray(int size) {
    byte[] array = new byte[size];
    double range = Byte.MAX_VALUE;
    for (int i = 0; i < array.length; i++) {
      array[i] = (byte) (Math.random() * range - range / 2.0);
    }
    return array;
  }

  private static char[] generateCharArray(int size) {
    char[] array = new char[size];
    double range = Character.MAX_VALUE;
    for (int i = 0; i < array.length; i++) {
      array[i] = (char) (Math.random() * range - range / 2.0);
    }
    return array;
  }

  private interface A {

    int getVal();
  }

  private static class ImplementsA implements A {

    int val;

    ImplementsA(int val) {
      this.val = val;
    }

    @Override
    public int getVal() {
      return val;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ImplementsA that = (ImplementsA) o;

      return val == that.val;
    }

    @Override
    public int hashCode() {
      return val;
    }
  }
}
