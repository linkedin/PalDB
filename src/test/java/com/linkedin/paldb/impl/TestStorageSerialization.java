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
import java.math.*;

import static org.junit.jupiter.api.Assertions.*;

class TestStorageSerialization {

  private Configuration configuration;
  private StorageSerialization serialization;

  @BeforeEach
  void setUp() {
    configuration = new Configuration();
    serialization = new StorageSerialization(configuration);
  }

  @Test
  void testCompressionEnabled() {
    assertFalse(serialization.isCompressionEnabled());
    Configuration config = new Configuration();
    config.set(Configuration.COMPRESSION_ENABLED, "true");
    StorageSerialization s = new StorageSerialization(config);
    assertTrue(s.isCompressionEnabled());
  }

  @Test
  void testSerializeKey() throws IOException {
    Integer l = 1;
    Object d = serialization.deserialize(serialization.serializeKey(l));
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
    assertEquals(l, serialization.deserialize(dis));
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
    assertEquals(l, serialization.deserialize(dis));
  }

  @Test
  void testSerializeKeyNull() {
    assertThrows(NullPointerException.class, () -> serialization.serializeKey(null));
  }

  @Test
  void testTransformValue() throws IOException {
    Integer l = 1;
    Object deserialize = serialization.deserialize(serialization.serializeValue(l));
    assertEquals(l, deserialize);
  }

  @Test
  void testTransformList() throws IOException {
    Integer[] l = new Integer[]{1, 2};
    Object deserialize = serialization.deserialize(serialization.serializeValue(l));
    assertEquals(int[].class, deserialize.getClass());
    assertArrayEquals(new int[]{1, 2}, (int[]) deserialize);
  }

  @Test
  void testTransformListWithNull() throws IOException {
    Integer[] l = new Integer[]{1, null, 2};
    Object deserialize = serialization.deserialize(serialization.serializeValue(l));
    assertEquals(int[].class, deserialize.getClass());
    assertArrayEquals(new int[]{1, 0, 2}, (int[])deserialize);
  }

  @Test
  void testTransformListOfList() throws IOException {
    Integer[][] l = new Integer[][]{{1}, {2}};
    Object deserialize = serialization.deserialize(serialization.serializeValue(l));
    assertEquals(int[][].class, deserialize.getClass());
    assertArrayEquals(new int[][]{{1}, {2}}, (int[][])deserialize);
  }

  @Test
  void testCustomSerializer() throws Throwable {
    configuration.registerSerializer(new Serializer<Point>() {
      @Override
      public void write(DataOutput dataOutput, Point input)
          throws IOException {
        dataOutput.writeInt(input.x);
        dataOutput.writeInt(input.y);
      }

      @Override
      public Point read(DataInput input)
          throws IOException {
        return new Point(input.readInt(), input.readInt());
      }

      @Override
      public Class<Point> serializedClass() {
        return Point.class;
      }

    });
    Point p = new Point(42, 9);
    byte[] buf = serialization.serialize(p);
    assertEquals(p, serialization.deserialize(buf));
  }

  @Test
  void testCustomArraySerializer() throws Throwable {
    configuration.registerSerializer(new Serializer<Point[]>() {
      @Override
      public void write(DataOutput dataOutput, Point[] input)
          throws IOException {
        dataOutput.writeInt(input[0].x);
        dataOutput.writeInt(input[0].y);
      }

      @Override
      public Point[] read(DataInput input)
          throws IOException {
        return new Point[]{new Point(input.readInt(), input.readInt())};
      }

      @Override
      public Class<Point[]> serializedClass() {
        return Point[].class;
      }

    });
    Point[] p = new Point[]{new Point(42, 9)};
    byte[] buf = serialization.serialize(p);
    assertArrayEquals(p, (Point[])serialization.deserialize(buf));
  }

  @Test
  void testInnerClassSerializer() throws Throwable {
    configuration.registerSerializer(new Serializer<ImplementsA>() {

      @Override
      public ImplementsA read(DataInput dataInput) throws IOException {
        return new ImplementsA(dataInput.readInt());
      }

      @Override
      public Class<ImplementsA> serializedClass() {
        return ImplementsA.class;
      }

      @Override
      public void write(DataOutput dataOutput, ImplementsA input) throws IOException {
        dataOutput.writeInt(input.getVal());
      }

    });
    ImplementsA a = new ImplementsA(42);
    byte[] buf = serialization.serialize(a);
    assertEquals(a, serialization.deserialize(buf));
  }

  @Test
  void testInheritedSerializer() {
    configuration.registerSerializer(new Serializer<A>() {

      @Override
      public A read(DataInput dataInput) throws IOException {
        return new ImplementsA(dataInput.readInt());
      }

      @Override
      public Class<A> serializedClass() {
        return A.class;
      }

      @Override
      public void write(DataOutput dataOutput, A input) throws IOException {
        dataOutput.writeInt(input.getVal());
      }

    });
    assertThrows(MissingSerializer.class, () -> {
      ImplementsA a = new ImplementsA(42);
      byte[] buf = serialization.serialize(a);
      assertEquals(a, serialization.deserialize(buf));
    });
  }

  @Test
  void testNull() throws Throwable {
    byte[] buf = serialization.serialize(null);
    assertNull(serialization.deserialize(buf));
  }

  @Test
  void testByte() throws Throwable {
    byte[] vals = new byte[]{-1, 0, 1, 6};
    for (byte val : vals) {
      byte[] buf = serialization.serialize(val);
      Object l2 = serialization.deserialize(buf);
      assertSame(Byte.class, l2.getClass());
      assertEquals(val, l2);
    }
  }

  @Test
  void testNotSupported() {
    assertThrows(MissingSerializer.class, () -> serialization.serialize(new Color(0, 0, 0)));
  }

  @Test
  void testInt() throws IOException {
    int[] vals = {Integer.MIN_VALUE,
        -Short.MIN_VALUE * 2, -Short.MIN_VALUE
        + 1, -Short.MIN_VALUE, -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 127, 254, 255, 256, Short.MAX_VALUE,
        Short.MAX_VALUE + 1, Short.MAX_VALUE * 2, Integer.MAX_VALUE};
    for (int i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      assertSame(Integer.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testShort() throws IOException {
    short[] vals = {(short) (-Short.MIN_VALUE
        + 1), (short) -Short.MIN_VALUE, -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 127, 254, 255, 256, Short.MAX_VALUE,
        Short.MAX_VALUE - 1, Short.MAX_VALUE};
    for (short i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      assertSame(Short.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testDouble() throws IOException {
    double[] vals = {1f, 0f, -1f, Math.PI, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1, -100};
    for (double i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      assertSame(Double.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testFloat() throws IOException {
    float[] vals = {1f, 0f, -1f, (float) Math.PI, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1, -100};
    for (float i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      assertSame(Float.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testChar() throws IOException {
    char[] vals = {'a', ' '};
    for (char i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
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
    for (long i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      assertSame(Long.class, l2.getClass());
      assertEquals(i, l2);
    }
  }

  @Test
  void testBoolean() throws IOException {
    byte[] buf = serialization.serialize(true);
    Object l2 = serialization.deserialize(buf);
    assertSame(Boolean.class, l2.getClass());
    assertEquals(true, l2);

    byte[] buf2 = serialization.serialize(false);
    Object l22 = serialization.deserialize(buf2);
    assertSame(Boolean.class, l22.getClass());
    assertEquals(false, l22);
  }

  @Test
  void testString() throws IOException {
    byte[] buf = serialization.serialize("Abcd");
    String l2 = (String) serialization.deserialize(buf);
    assertEquals("Abcd", l2);
  }

  @Test
  void testEmptyString() throws IOException {
    byte[] buf = serialization.serialize("");
    String l2 = (String) serialization.deserialize(buf);
    assertEquals("", l2);
  }

  @Test
  void testBigString() throws IOException {
    var bigString = new StringBuilder();
    for (int i = 0; i < 1e4; i++) {
      bigString.append(i % 10);
    }
    byte[] buf = serialization.serialize(bigString.toString());
    String l2 = (String) serialization.deserialize(buf);
    assertEquals(bigString.toString(), l2);
  }

  @Test
  void testClass() throws IOException {
    byte[] buf = serialization.serialize(String.class);
    Class l2 = (Class) serialization.deserialize(buf);
    assertEquals(String.class, l2);
  }

  @Test
  void testClass2() throws IOException {
    byte[] buf = serialization.serialize(long[].class);
    Class l2 = (Class) serialization.deserialize(buf);
    assertEquals(long[].class, l2);
  }

  @Test
  void testUnicodeString() throws IOException {
    String s = "Ciudad Bolíva";
    byte[] buf = serialization.serialize(s);
    Object l2 = serialization.deserialize(buf);
    assertEquals(s, l2);
  }

  @Test
  void testStringArray() throws IOException {
    String[] l = new String[]{"foo", "bar", ""};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    assertArrayEquals(l, (String[]) deserialize);
  }

  @Test
  void testObjectArray() throws IOException {
    Object[] l = new Object[]{"foo", 2, Boolean.TRUE};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    assertArrayEquals(l, (Object[]) deserialize);
  }

  @Test
  void testBooleanArray() throws IOException {
    boolean[] l = new boolean[]{true, false};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    assertArrayEquals(l, (boolean[]) deserialize);
  }

  @Test
  void testDoubleArray() throws IOException {
    double[] l = new double[]{Math.PI, 1D};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    assertArrayEquals(l, (double[]) deserialize);
  }

  @Test
  void testFloatArray() throws IOException {
    float[] l = new float[]{1F, 1.234235F};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    assertArrayEquals(l, (float[]) deserialize);
  }

  @Test
  void testByteArray() throws IOException {
    byte[] l = new byte[]{1, 34, -5};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    assertArrayEquals(l, (byte[]) deserialize);
  }

  @Test
  void testShortArray()
          throws Exception {
    short[] l = new short[]{1, 345, -5000};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    assertArrayEquals(l, (short[]) deserialize);
  }

  @Test
  void testCharArray()
      throws IOException {
    char[] l = new char[]{'1', 'a', '&'};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    assertArrayEquals(l, (char[]) deserialize);
  }

  @Test
  void testIntArray()
      throws IOException {
    int[][] l = new int[][]{{3, 5}, {-1200, 29999}, {3, 100000}, {-43999, 100000}};
    for (int[] a : l) {
      Object deserialize = serialization.deserialize(serialization.serialize(a));
      assertArrayEquals(a, (int[]) deserialize);
    }
  }

  @Test
  void testLongArray()
      throws IOException {
    long[][] l = new long[][]{{3L, 5L}, {-1200L, 29999L}, {3L, 100000L}, {-43999L, 100000L}, {-123L, 12345678901234L}};
    for (long[] a : l) {
      Object deserialize = serialization.deserialize(serialization.serialize(a));
      assertArrayEquals(a, (long[]) deserialize);
    }
  }

  @Test
  void testDoubleCompressedArray()
      throws IOException {
    double[] l = generateDoubleArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    assertArrayEquals(l, (double[]) deserialize);
  }

  @Test
  void testFloatCompressedArray()
      throws IOException {
    float[] l = generateFloatArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    assertArrayEquals(l, (float[]) deserialize);
  }

  @Test
  void testByteCompressedArray()
      throws IOException {
    byte[] l = generateByteArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    assertArrayEquals(l, (byte[]) deserialize);
  }

  @Test
  void testCharCompressedArray()
      throws IOException {
    char[] l = generateCharArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    assertArrayEquals(l, (char[]) deserialize);
  }

  @Test
  void testShortCompressedArray()
      throws IOException {
    short[] l = generateShortArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    assertArrayEquals(l, (short[]) deserialize);
  }

  @Test
  void testIntCompressedArray()
      throws IOException {
    int[] l = generateIntArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    assertArrayEquals(l, (int[]) deserialize);
  }

  @Test
  void testLongCompressedArray()
      throws IOException {
    long[] l = generateLongArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    assertArrayEquals(l, (long[]) deserialize);
  }

  @Test
  void testBigDecimal()
      throws IOException {
    BigDecimal d = new BigDecimal("445656.7889889895165654423236");
    assertEquals(d, serialization.deserialize(serialization.serialize(d)));
    d = new BigDecimal("-53534534534534445656.7889889895165654423236");
    assertEquals(d, serialization.deserialize(serialization.serialize(d)));
  }

  @Test
  void testBigInteger()
      throws IOException {
    BigInteger d = new BigInteger("4456567889889895165654423236");
    assertEquals(d, serialization.deserialize(serialization.serialize(d)));
    d = new BigInteger("-535345345345344456567889889895165654423236");
    assertEquals(d, serialization.deserialize(serialization.serialize(d)));
  }

  @Test
  void testMultiDimensionalIntArray()
      throws IOException {
    int[][] d = new int[2][];
    d[0] = new int[]{1, 3};
    d[1] = new int[]{-3, 1};
    Object res = serialization.deserialize(serialization.serialize(d));
    assertEquals(res.getClass(), int[][].class);
    assertArrayEquals(d, (int[][])res);
  }

  @Test
  void testMultiDimensionalLongArray()
      throws IOException {
    long[][] d = new long[2][];
    d[0] = new long[]{1, 3};
    d[1] = new long[]{-3, 1};
    Object res = serialization.deserialize(serialization.serialize(d));
    assertEquals(res.getClass(), long[][].class);
    assertArrayEquals(d, (long[][])res);
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
