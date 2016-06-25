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
import com.linkedin.paldb.api.Serializer;
import com.linkedin.paldb.api.UnsupportedTypeException;

import java.awt.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestStorageSerialization {

  private Configuration configuration;
  private StorageSerialization serialization;

  @BeforeMethod
  public void setUp() {
    configuration = new Configuration();
    serialization = new StorageSerialization(configuration);
  }

  @Test
  public void testCompressionEnabled() {
    Assert.assertFalse(serialization.isCompressionEnabled());
    Configuration config = new Configuration();
    config.set(Configuration.COMPRESSION_ENABLED, "true");
    StorageSerialization s = new StorageSerialization(config);
    Assert.assertTrue(s.isCompressionEnabled());
  }

  @Test
  public void testSerializeKey() throws IOException, ClassNotFoundException {
    Integer l = 1;
    Object d = serialization.deserialize(serialization.serializeKey(l));
    Assert.assertEquals(d, l);
  }

  @Test
  public void testSerializeKeyDataOutput() throws IOException, ClassNotFoundException {
    Integer l = 1;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    serialization.serializeKey(l, dos);
    dos.close();
    bos.close();

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream dis = new DataInputStream(bis);
    Assert.assertEquals(serialization.deserialize(dis), l);
  }

  @Test
  public void testSerializeValueDataOutput() throws IOException, ClassNotFoundException {
    Integer l = 1;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    serialization.serializeValue(l, dos);
    dos.close();
    bos.close();

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream dis = new DataInputStream(bis);
    Assert.assertEquals(serialization.deserialize(dis), l);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testSerializeKeyNull() throws IOException, ClassNotFoundException {
    serialization.serializeKey(null);
  }

  @Test
  public void testTransformValue()
      throws ClassNotFoundException, IOException {
    Integer l = 1;
    Object deserialize = serialization.deserialize(serialization.serializeValue(l));
    Assert.assertEquals(deserialize, l);
  }

  @Test
  public void testTransformList()
      throws ClassNotFoundException, IOException {
    Integer[] l = new Integer[]{1, 2};
    Object deserialize = serialization.deserialize(serialization.serializeValue(l));
    Assert.assertEquals(deserialize.getClass(), int[].class);
    Assert.assertEquals(deserialize, new int[]{1, 2});
  }

  @Test
  public void testTransformListWithNull()
      throws ClassNotFoundException, IOException {
    Integer[] l = new Integer[]{1, null, 2};
    Object deserialize = serialization.deserialize(serialization.serializeValue(l));
    Assert.assertEquals(deserialize.getClass(), int[].class);
    Assert.assertEquals(deserialize, new int[]{1, 0, 2});
  }

  @Test
  public void testTransformListOfList()
      throws ClassNotFoundException, IOException {
    Integer[][] l = new Integer[][]{{1}, {2}};
    Object deserialize = serialization.deserialize(serialization.serializeValue(l));
    Assert.assertEquals(deserialize.getClass(), int[][].class);
    Assert.assertEquals(deserialize, new int[][]{{1}, {2}});
  }

  @Test
  public void testCustomSerializer()
      throws Throwable {
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
      public int getWeight(Point instance) {
        return 0;
      }
    });
    Point p = new Point(42, 9);
    byte[] buf = serialization.serialize(p);
    Assert.assertEquals(serialization.deserialize(buf), p);
  }

  @Test
  public void testCustomArraySerializer()
      throws Throwable {
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
      public int getWeight(Point[] instance) {
        return 0;
      }
    });
    Point[] p = new Point[]{new Point(42, 9)};
    byte[] buf = serialization.serialize(p);
    Assert.assertEquals(serialization.deserialize(buf), p);
  }

  @Test
  public void testInnerClassSerializer() throws Throwable {
    configuration.registerSerializer(new Serializer<ImplementsA>() {

      @Override
      public ImplementsA read(DataInput dataInput) throws IOException {
        return new ImplementsA(dataInput.readInt());
      }

      @Override
      public void write(DataOutput dataOutput, ImplementsA input) throws IOException {
        dataOutput.writeInt(input.getVal());
      }

      @Override
      public int getWeight(ImplementsA instance) {
        return 0;
      }
    });
    ImplementsA a = new ImplementsA(42);
    byte[] buf = serialization.serialize(a);
    Assert.assertEquals(serialization.deserialize(buf), a);
  }

  @Test
  public void testInheritedSerializer() throws Throwable {
    configuration.registerSerializer(new Serializer<A>() {

      @Override
      public A read(DataInput dataInput) throws IOException {
        return new ImplementsA(dataInput.readInt());
      }

      @Override
      public void write(DataOutput dataOutput, A input) throws IOException {
        dataOutput.writeInt(input.getVal());
      }

      @Override
      public int getWeight(A instance) {
        return 0;
      }
    });
    ImplementsA a = new ImplementsA(42);
    byte[] buf = serialization.serialize(a);
    Assert.assertEquals(serialization.deserialize(buf), a);
  }

  @Test
  public void testNull()
      throws Throwable {
    byte[] buf = serialization.serialize(null);
    Assert.assertNull(serialization.deserialize(buf));
  }

  @Test
  public void testByte()
      throws Throwable {
    byte[] vals = new byte[]{-1, 0, 1, 6};
    for (byte val : vals) {
      byte[] buf = serialization.serialize(val);
      Object l2 = serialization.deserialize(buf);
      Assert.assertTrue(l2.getClass() == Byte.class);
      Assert.assertEquals(l2, val);
    }
  }

  @Test(expectedExceptions = UnsupportedTypeException.class)
  public void testNotSupported()
      throws Throwable {
    serialization.serialize(new Color(0, 0, 0));
  }

  @Test
  public void testInt()
      throws IOException, ClassNotFoundException {
    int[] vals = {Integer.MIN_VALUE,
        -Short.MIN_VALUE * 2, -Short.MIN_VALUE
        + 1, -Short.MIN_VALUE, -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 127, 254, 255, 256, Short.MAX_VALUE,
        Short.MAX_VALUE + 1, Short.MAX_VALUE * 2, Integer.MAX_VALUE};
    for (int i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      Assert.assertTrue(l2.getClass() == Integer.class);
      Assert.assertEquals(l2, i);
    }
  }

  @Test
  public void testShort()
      throws IOException, ClassNotFoundException {
    short[] vals = {(short) (-Short.MIN_VALUE
        + 1), (short) -Short.MIN_VALUE, -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 127, 254, 255, 256, Short.MAX_VALUE,
        Short.MAX_VALUE - 1, Short.MAX_VALUE};
    for (short i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      Assert.assertTrue(l2.getClass() == Short.class);
      Assert.assertEquals(l2, i);
    }
  }

  @Test
  public void testDouble()
      throws IOException, ClassNotFoundException {
    double[] vals = {1f, 0f, -1f, Math.PI, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1, -100};
    for (double i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      Assert.assertTrue(l2.getClass() == Double.class);
      Assert.assertEquals(l2, i);
    }
  }

  @Test
  public void testFloat()
      throws IOException, ClassNotFoundException {
    float[] vals = {1f, 0f, -1f, (float) Math.PI, 255, 256, Short.MAX_VALUE, Short.MAX_VALUE + 1, -100};
    for (float i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      Assert.assertTrue(l2.getClass() == Float.class);
      Assert.assertEquals(l2, i);
    }
  }

  @Test
  public void testChar()
      throws IOException, ClassNotFoundException {
    char[] vals = {'a', ' '};
    for (char i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      Assert.assertTrue(l2.getClass() == Character.class);
      Assert.assertEquals(l2, i);
    }
  }

  @Test
  public void testLong()
      throws IOException, ClassNotFoundException {
    long[] vals = {Long.MIN_VALUE, Integer.MIN_VALUE,
        Integer.MIN_VALUE - 1,
        Integer.MIN_VALUE + 1,
        -Short.MIN_VALUE * 2, -Short.MIN_VALUE
        + 1, -Short.MIN_VALUE, -10, -9, -8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 127, 254, 255, 256, Short.MAX_VALUE,
        Short.MAX_VALUE + 1, Short.MAX_VALUE * 2, Integer.MAX_VALUE, Integer.MAX_VALUE + 1, Long.MAX_VALUE};
    for (long i : vals) {
      byte[] buf = serialization.serialize(i);
      Object l2 = serialization.deserialize(buf);
      Assert.assertTrue(l2.getClass() == Long.class);
      Assert.assertEquals(l2, i);
    }
  }

  @Test
  public void testBoolean()
      throws IOException, ClassNotFoundException {
    byte[] buf = serialization.serialize(true);
    Object l2 = serialization.deserialize(buf);
    Assert.assertTrue(l2.getClass() == Boolean.class);
    Assert.assertEquals(l2, true);

    byte[] buf2 = serialization.serialize(false);
    Object l22 = serialization.deserialize(buf2);
    Assert.assertTrue(l22.getClass() == Boolean.class);
    Assert.assertEquals(l22, false);
  }

  @Test
  public void testString()
      throws IOException, ClassNotFoundException {
    byte[] buf = serialization.serialize("Abcd");
    String l2 = (String) serialization.deserialize(buf);
    Assert.assertEquals(l2, "Abcd");
  }

  @Test
  public void testEmptyString()
      throws IOException, ClassNotFoundException {
    byte[] buf = serialization.serialize("");
    String l2 = (String) serialization.deserialize(buf);
    Assert.assertEquals(l2, "");
  }

  @Test
  public void testBigString()
      throws IOException, ClassNotFoundException {
    String bigString = "";
    for (int i = 0; i < 1e4; i++) {
      bigString += i % 10;
    }
    byte[] buf = serialization.serialize(bigString);
    String l2 = (String) serialization.deserialize(buf);
    Assert.assertEquals(l2, bigString);
  }

  @Test
  public void testClass()
      throws IOException, ClassNotFoundException {
    byte[] buf = serialization.serialize(String.class);
    Class l2 = (Class) serialization.deserialize(buf);
    Assert.assertEquals(l2, String.class);
  }

  @Test
  public void testClass2()
      throws IOException, ClassNotFoundException {
    byte[] buf = serialization.serialize(long[].class);
    Class l2 = (Class) serialization.deserialize(buf);
    Assert.assertEquals(l2, long[].class);
  }

  @Test
  public void testUnicodeString()
      throws ClassNotFoundException, IOException {
    String s = "Ciudad Bolíva";
    byte[] buf = serialization.serialize(s);
    Object l2 = serialization.deserialize(buf);
    Assert.assertEquals(l2, s);
  }

  @Test
  public void testStringArray()
      throws ClassNotFoundException, IOException {
    String[] l = new String[]{"foo", "bar", ""};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    Assert.assertTrue(Arrays.equals(l, (String[]) deserialize));
  }

  @Test
  public void testObjectArray()
      throws ClassNotFoundException, IOException {
    Object[] l = new Object[]{"foo", 2, Boolean.TRUE};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    Assert.assertTrue(Arrays.equals(l, (Object[]) deserialize));
  }

  @Test
  public void testBooleanArray()
      throws ClassNotFoundException, IOException {
    boolean[] l = new boolean[]{true, false};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    Assert.assertTrue(Arrays.equals(l, (boolean[]) deserialize));
  }

  @Test
  public void testDoubleArray()
      throws ClassNotFoundException, IOException {
    double[] l = new double[]{Math.PI, 1D};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    Assert.assertTrue(Arrays.equals(l, (double[]) deserialize));
  }

  @Test
  public void testFloatArray()
      throws ClassNotFoundException, IOException {
    float[] l = new float[]{1F, 1.234235F};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    Assert.assertTrue(Arrays.equals(l, (float[]) deserialize));
  }

  @Test
  public void testByteArray()
      throws ClassNotFoundException, IOException {
    byte[] l = new byte[]{1, 34, -5};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    Assert.assertTrue(Arrays.equals(l, (byte[]) deserialize));
  }

  @Test
  public void testShortArray()
      throws ClassNotFoundException, IOException {
    short[] l = new short[]{1, 345, -5000};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    Assert.assertTrue(Arrays.equals(l, (short[]) deserialize));
  }

  @Test
  public void testCharArray()
      throws ClassNotFoundException, IOException {
    char[] l = new char[]{'1', 'a', '&'};
    Object deserialize = serialization.deserialize(serialization.serialize(l));
    Assert.assertTrue(Arrays.equals(l, (char[]) deserialize));
  }

  @Test
  public void testIntArray()
      throws ClassNotFoundException, IOException {
    int[][] l = new int[][]{{3, 5}, {-1200, 29999}, {3, 100000}, {-43999, 100000}};
    for (int[] a : l) {
      Object deserialize = serialization.deserialize(serialization.serialize(a));
      Assert.assertTrue(Arrays.equals(a, (int[]) deserialize));
    }
  }

  @Test
  public void testLongArray()
      throws ClassNotFoundException, IOException {
    long[][] l = new long[][]{{3l, 5l}, {-1200l, 29999l}, {3l, 100000l}, {-43999l, 100000l}, {-123l, 12345678901234l}};
    for (long[] a : l) {
      Object deserialize = serialization.deserialize(serialization.serialize(a));
      Assert.assertTrue(Arrays.equals(a, (long[]) deserialize));
    }
  }

  @Test
  public void testDoubleCompressedArray()
      throws ClassNotFoundException, IOException {
    double[] l = generateDoubleArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    Assert.assertTrue(Arrays.equals(l, (double[]) deserialize));
  }

  @Test
  public void testFloatCompressedArray()
      throws ClassNotFoundException, IOException {
    float[] l = generateFloatArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    Assert.assertTrue(Arrays.equals(l, (float[]) deserialize));
  }

  @Test
  public void testByteCompressedArray()
      throws ClassNotFoundException, IOException {
    byte[] l = generateByteArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    Assert.assertTrue(Arrays.equals(l, (byte[]) deserialize));
  }

  @Test
  public void testCharCompressedArray()
      throws ClassNotFoundException, IOException {
    char[] l = generateCharArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    Assert.assertTrue(Arrays.equals(l, (char[]) deserialize));
  }

  @Test
  public void testShortCompressedArray()
      throws ClassNotFoundException, IOException {
    short[] l = generateShortArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    Assert.assertTrue(Arrays.equals(l, (short[]) deserialize));
  }

  @Test
  public void testIntCompressedArray()
      throws ClassNotFoundException, IOException {
    int[] l = generateIntArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    Assert.assertTrue(Arrays.equals(l, (int[]) deserialize));
  }

  @Test
  public void testLongCompressedArray()
      throws ClassNotFoundException, IOException {
    long[] l = generateLongArray(500);
    Object deserialize = serialization.deserialize(serialization.serialize(l, true));
    Assert.assertTrue(Arrays.equals(l, (long[]) deserialize));
  }

  @Test
  public void testBigDecimal()
      throws IOException, ClassNotFoundException {
    BigDecimal d = new BigDecimal("445656.7889889895165654423236");
    Assert.assertEquals(d, serialization.deserialize(serialization.serialize(d)));
    d = new BigDecimal("-53534534534534445656.7889889895165654423236");
    Assert.assertEquals(d, serialization.deserialize(serialization.serialize(d)));
  }

  @Test
  public void testBigInteger()
      throws IOException, ClassNotFoundException {
    BigInteger d = new BigInteger("4456567889889895165654423236");
    Assert.assertEquals(d, serialization.deserialize(serialization.serialize(d)));
    d = new BigInteger("-535345345345344456567889889895165654423236");
    Assert.assertEquals(d, serialization.deserialize(serialization.serialize(d)));
  }

  @Test
  public void testMultiDimensionalIntArray()
      throws IOException, ClassNotFoundException {
    int[][] d = new int[2][];
    d[0] = new int[]{1, 3};
    d[1] = new int[]{-3, 1};
    Object res = serialization.deserialize(serialization.serialize(d));
    Assert.assertEquals(res.getClass(), int[][].class);
    Assert.assertEquals(d, res);
  }

  @Test
  public void testMultiDimensionalLongArray()
      throws IOException, ClassNotFoundException {
    long[][] d = new long[2][];
    d[0] = new long[]{1, 3};
    d[1] = new long[]{-3, 1};
    Object res = serialization.deserialize(serialization.serialize(d));
    Assert.assertEquals(res.getClass(), long[][].class);
    Assert.assertEquals(d, res);
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

  private static interface A {

    int getVal();
  }

  private static class ImplementsA implements A {

    int val;

    public ImplementsA(int val) {
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
