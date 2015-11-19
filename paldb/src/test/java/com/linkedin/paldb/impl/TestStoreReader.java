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

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestStoreReader {

  private final File STORE_FOLDER = new File("data");
  private final File STORE_FILE = new File(STORE_FOLDER, "paldb.dat");
  private StoreReader reader;

  private final Object[] testValues =
      new Object[]{true, (byte) 1, 'a', 1.0, 1f, (short) 1, 1, 1l, "foo", new boolean[]{true}, new byte[]{1}, new char[]{'a'}, new double[]{1.0}, new float[]{1f}, new short[]{1}, new int[]{1}, new long[]{1l}, new String[]{"foo"}, new Object[]{"foo"}, new Point(
          4, 56)};

  @BeforeMethod
  public void setUp() {
    STORE_FILE.delete();
    STORE_FOLDER.delete();
    STORE_FOLDER.mkdir();

    Configuration configuration = new Configuration();
    configuration.registerSerializer(new PointSerializer());
    StoreWriter writer = PalDB.createWriter(STORE_FILE, configuration);
    for (int i = 0; i < testValues.length; i++) {
      writer.put(i, testValues[i]);
    }
    writer.close();

    reader = PalDB.createReader(STORE_FILE, new Configuration());
  }

  @AfterMethod
  public void cleanUp() {
    try {
      reader.close();
    } catch (Exception e) {
    }
    STORE_FILE.delete();
    STORE_FOLDER.delete();
  }

  @Test
  public void testFile() {
    Assert.assertEquals(reader.getFile(), STORE_FILE);
  }

  @Test
  public void testSize() {
    Assert.assertEquals(reader.size(), testValues.length);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testStoreClosed() {
    reader.close();
    reader.get(0);
  }

  @Test
  public void testGetBoolean()
      throws Throwable {
    Assert.assertTrue(reader.getBoolean(0));
    Assert.assertTrue(reader.getBoolean(0, false));
    Assert.assertFalse(reader.getBoolean(-1, false));
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetBooleanMissing()
      throws Throwable {
    reader.getBoolean(-1);
  }

  @Test
  public void testGetByte()
      throws Throwable {
    Assert.assertEquals(reader.getByte(1), (byte) 1);
    Assert.assertEquals(reader.getByte(1, (byte) 5), (byte) 1);
    Assert.assertEquals(reader.getByte(-1, (byte) 5), (byte) 5);
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetByteMissing()
      throws Throwable {
    reader.getByte(-1);
  }

  @Test
  public void testGetChar()
      throws Throwable {
    Assert.assertEquals(reader.getChar(2), (char) 'a');
    Assert.assertEquals(reader.getChar(2, (char) 'b'), (char) 'a');
    Assert.assertEquals(reader.getChar(-1, (char) 'b'), (char) 'b');
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetCharMissing()
      throws Throwable {
    reader.getChar(-1);
  }

  @Test
  public void testGetDouble()
      throws Throwable {
    Assert.assertEquals(reader.getDouble(3), 1.0);
    Assert.assertEquals(reader.getDouble(3, 2.0), 1.0);
    Assert.assertEquals(reader.getDouble(-1, 2.0), 2.0);
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetDoubleMissing()
      throws Throwable {
    reader.getDouble(-1);
  }

  @Test
  public void testGetFloat()
      throws Throwable {
    Assert.assertEquals(reader.getFloat(4), 1f);
    Assert.assertEquals(reader.getFloat(4, 2f), 1f);
    Assert.assertEquals(reader.getFloat(-1, 2f), 2f);
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetFloatMissing()
      throws Throwable {
    reader.getFloat(-1);
  }

  @Test
  public void testGetShort()
      throws Throwable {
    Assert.assertEquals(reader.getShort(5), (short) 1);
    Assert.assertEquals(reader.getShort(5, (short) 2), (short) 1);
    Assert.assertEquals(reader.getShort(-1, (short) 2), (short) 2);
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetShortMissing()
      throws Throwable {
    reader.getShort(-1);
  }

  @Test
  public void testGetInt()
      throws Throwable {
    Assert.assertEquals(reader.getInt(6), 1);
    Assert.assertEquals(reader.getInt(6, 2), 1);
    Assert.assertEquals(reader.getInt(-1, 2), 2);
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetIntMissing()
      throws Throwable {
    reader.getInt(-1);
  }

  @Test
  public void testGetLong()
      throws Throwable {
    Assert.assertEquals(reader.getLong(7), 1l);
    Assert.assertEquals(reader.getLong(7, 2l), 1l);
    Assert.assertEquals(reader.getLong(-1, 2l), 2l);
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetLongMissing()
      throws Throwable {
    reader.getLong(-1);
  }

  @Test
  public void testGetString()
      throws Throwable {
    Assert.assertEquals(reader.getString(8), "foo");
    Assert.assertEquals(reader.getString(8, "bar"), "foo");
    Assert.assertEquals(reader.getString(-1, "bar"), "bar");
  }

  @Test
  public void testGetStringMissing()
      throws Throwable {
    Assert.assertNull(reader.getString(-1));
  }

  @Test
  public void testGetBooleanArray()
      throws Throwable {
    Assert.assertEquals(reader.getBooleanArray(9), new boolean[]{true});
    Assert.assertEquals(reader.getBooleanArray(9, new boolean[]{false}), new boolean[]{true});
    Assert.assertEquals(reader.getBooleanArray(-1, new boolean[]{false}), new boolean[]{false});
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetBooleanArrayMissing()
      throws Throwable {
    reader.getBooleanArray(-1);
  }

  @Test
  public void testGetByteArray()
      throws Throwable {
    Assert.assertEquals(reader.getByteArray(10), new byte[]{1});
    Assert.assertEquals(reader.getByteArray(10, new byte[]{2}), new byte[]{1});
    Assert.assertEquals(reader.getByteArray(-1, new byte[]{2}), new byte[]{2});
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetByteArrayMissing()
      throws Throwable {
    reader.getByteArray(-1);
  }

  @Test
  public void testGetCharArray()
      throws Throwable {
    Assert.assertEquals(reader.getCharArray(11), new char[]{'a'});
    Assert.assertEquals(reader.getCharArray(11, new char[]{'b'}), new char[]{'a'});
    Assert.assertEquals(reader.getCharArray(-1, new char[]{'b'}), new char[]{'b'});
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetCharArrayMissing()
      throws Throwable {
    reader.getCharArray(-1);
  }

  @Test
  public void testGetDoubleArray()
      throws Throwable {
    Assert.assertEquals(reader.getDoubleArray(12), new double[]{1.0});
    Assert.assertEquals(reader.getDoubleArray(12, new double[]{2.0}), new double[]{1.0});
    Assert.assertEquals(reader.getDoubleArray(-1, new double[]{2.0}), new double[]{2.0});
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetDoubleArrayMissing()
      throws Throwable {
    reader.getDoubleArray(-1);
  }

  @Test
  public void testGetFloatArray()
      throws Throwable {
    Assert.assertEquals(reader.getFloatArray(13), new float[]{1f});
    Assert.assertEquals(reader.getFloatArray(13, new float[]{2f}), new float[]{1f});
    Assert.assertEquals(reader.getFloatArray(-1, new float[]{2f}), new float[]{2f});
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetFloatArrayMissing()
      throws Throwable {
    reader.getFloatArray(-1);
  }

  @Test
  public void testGetShortArray()
      throws Throwable {
    Assert.assertEquals(reader.getShortArray(14), new short[]{1});
    Assert.assertEquals(reader.getShortArray(14, new short[]{2}), new short[]{1});
    Assert.assertEquals(reader.getShortArray(-1, new short[]{2}), new short[]{2});
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetShortArrayMissing()
      throws Throwable {
    reader.getShortArray(-1);
  }

  @Test
  public void testGetIntArray()
      throws Throwable {
    Assert.assertEquals(reader.getIntArray(15), new int[]{1});
    Assert.assertEquals(reader.getIntArray(15, new int[]{2}), new int[]{1});
    Assert.assertEquals(reader.getIntArray(-1, new int[]{2}), new int[]{2});
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetIntArrayMissing()
      throws Throwable {
    reader.getIntArray(-1);
  }

  @Test
  public void testGetLongArray()
      throws Throwable {
    Assert.assertEquals(reader.getLongArray(16), new long[]{1l});
    Assert.assertEquals(reader.getLongArray(16, new long[]{2l}), new long[]{1l});
    Assert.assertEquals(reader.getLongArray(-1, new long[]{2l}), new long[]{2l});
  }

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetLongArrayMissing()
      throws Throwable {
    reader.getLongArray(-1);
  }

  @Test
  public void testGetStringArray()
      throws Throwable {
    Assert.assertEquals(reader.getStringArray(17), new String[]{"foo"});
    Assert.assertEquals(reader.getStringArray(17, new String[]{"bar"}), new String[]{"foo"});
    Assert.assertEquals(reader.getStringArray(-1, new String[]{"bar"}), new String[]{"bar"});
  }

  @Test
  public void testGetStringArrayMissing()
      throws Throwable {
    Assert.assertNull(reader.getStringArray(-1));
  }

  @Test
  public void testGetMissing()
      throws Throwable {
    Assert.assertNull(reader.get(-1));
  }

  @Test
  public void testGetArray()
      throws Throwable {
    Assert.assertEquals(reader.getArray(18), new Object[]{"foo"});
    Assert.assertEquals(reader.getArray(18, new Object[]{"bar"}), new Object[]{"foo"});
    Assert.assertEquals(reader.getArray(-1, new Object[]{"bar"}), new Object[]{"bar"});
  }

  @Test
  public void testGetArrayMissing()
      throws Throwable {
    Assert.assertNull(reader.getArray(-1));
  }

  @Test
  public void testGetPoint()
      throws Throwable {
    Assert.assertEquals(reader.get(19), new Point(4, 56));
  }

  @Test
  public void testIterator() {
    Iterable<Map.Entry<Integer, Object>> iter = reader.iterable();
    Assert.assertNotNull(iter);
    Iterator<Map.Entry<Integer, Object>> itr = iter.iterator();
    Assert.assertNotNull(itr);

    for (int i = 0; i < testValues.length; i++) {
      Assert.assertTrue(itr.hasNext());
      Map.Entry<Integer, Object> v = itr.next();
      Object val = testValues[v.getKey()];
      Assert.assertEquals(v.getValue(), val);
    }
  }

  @Test
  public void testKeyIterator() {
    Iterable<Integer> iter = reader.keys();
    Assert.assertNotNull(iter);
    Iterator<Integer> itr = iter.iterator();
    Assert.assertNotNull(itr);

    Set<Integer> actual = new HashSet<Integer>();
    Set<Integer> expected = new HashSet<Integer>();
    for (int i = 0; i < testValues.length; i++) {
      Assert.assertTrue(itr.hasNext());
      Integer k = itr.next();
      actual.add(k);
      expected.add(i);
    }
    Assert.assertEquals(actual, expected);
  }

  // UTILITY

  public static class PointSerializer implements Serializer<Point> {

    @Override
    public Point read(DataInput input)
        throws IOException {
      return new Point(input.readInt(), input.readInt());
    }

    @Override
    public void write(DataOutput output, Point input)
        throws IOException {
      output.writeInt(input.x);
      output.writeInt(input.y);
    }

    @Override
    public int getWeight(Point instance) {
      return 8;
    }
  }
}
