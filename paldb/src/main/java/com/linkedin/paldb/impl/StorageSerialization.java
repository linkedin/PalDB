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
import com.linkedin.paldb.utils.DataInputOutput;
import com.linkedin.paldb.utils.LongPacker;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.xerial.snappy.Snappy;

/**
 * Internal serialization implementation.
 */
public final class StorageSerialization {

  //Buffer
  private final DataInputOutput dataInputOutput = new DataInputOutput();
  //Compression
  private final boolean compression;
  //Serializers
  private Serializers serializers;

  /**
   * Default constructor with configuration.
   * <p>
   * Uses <code>Configuration.COMPRESSION_ENABLED</code> and <code>Configuration.KEY_COMPARATOR</code> values
   * from the configuration.
   *
   * @param config configuration
   */
  public StorageSerialization(Configuration config) {
    this.compression = config.getBoolean(Configuration.COMPRESSION_ENABLED);
    this.serializers = config.getSerializers();
  }

  /**
   * Serializes the key object and returns it as a byte array.
   *
   * @param key key to serialize
   * @return key as byte array
   * @throws IOException if an io error occurs
   */
  public byte[] serializeKey(Object key)
      throws IOException {
    if (key == null) {
      throw new NullPointerException();
    }
    serializeObject(key, dataInputOutput.reset(), false);
    return dataInputOutput.toByteArray();
  }

  /**
   * Serializes the key and writes it into <code>dataOutput</code>.
   *
   * @param key key to serialize
   * @param dataOutput data output
   * @throws IOException if an io error occurs
   */
  public void serializeKey(Object key, DataOutput dataOutput)
      throws IOException {
    serializeObject(key, dataOutput, false);
  }

  /**
   * Serializes the value object and returns it as a byte array.
   *
   * @param value value to serialize
   * @return value as byte array
   * @throws IOException if an io error occurs
   */
  public byte[] serializeValue(Object value)
      throws IOException {

    serializeObject(value, dataInputOutput.reset(), compression);
    return dataInputOutput.toByteArray();
  }

  /**
   * Serializes the value and writes it into <code>dataOutput</code>.
   *
   * @param value value to serialize
   * @param dataOutput data output
   * @throws IOException if an io error occurs
   */
  public void serializeValue(Object value, DataOutput dataOutput)
      throws IOException {
    serializeObject(value, dataOutput, compression);
  }

  /**
   * Serialization implementation.
   *
   * @param obj object to serialize
   * @param useCompression use compression
   * @return serialized object in bytes
   * @throws IOException if an io error occurs
   */
  private void serializeObject(Object obj, DataOutput dataOutput, boolean useCompression)
      throws IOException {
    //Cast to primitive arrays if necessary
    if (obj != null && obj.getClass().isArray()) {
      if (obj instanceof Integer[]) {
        obj = (int[]) getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Boolean[]) {
        obj = (boolean[]) getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Byte[]) {
        obj = (byte[]) getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Character[]) {
        obj = (char[]) getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Double[]) {
        obj = (double[]) getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Float[]) {
        obj = (float[]) getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Long[]) {
        obj = (long[]) getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Short[]) {
        obj = (short[]) getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Integer[][]) {
        obj = (int[][]) getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Boolean[][]) {
        obj = (boolean[][]) getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Byte[][]) {
        obj = (byte[][]) getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Character[][]) {
        obj = (char[][]) getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Double[][]) {
        obj = (double[][]) getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Float[][]) {
        obj = (float[][]) getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Long[][]) {
        obj = (long[][]) getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Short[][]) {
        obj = (short[][]) getPrimitiveArray((Object[][]) obj);
      }
    }

    serialize(dataOutput, obj, useCompression);
  }

  /**
   * Returns true if compression is enabled.
   *
   * @return true if enabled, false otherwise
   */
  public boolean isCompressionEnabled() {
    return compression;
  }

  // UTILITIES

  private static Object getPrimitiveArray(Object[][] array) {
    Class arrayClass = array.getClass().getComponentType().getComponentType();
    if (!arrayClass.isPrimitive()) {
      Class primitiveClass = getPrimitiveType(arrayClass);

      int arrayLength = array.length;
      Object primitiveArray = Array.newInstance(primitiveClass, arrayLength, 0);

      for (int i = 0; i < arrayLength; i++) {
        Object[] obj = array[i];
        if (obj != null) {
          Object innerArray = Array.newInstance(primitiveClass, obj.length);
          for (int j = 0; j < obj.length; j++) {
            Object iobj = obj[j];
            if (iobj != null) {
              Array.set(innerArray, j, iobj);
            }
          }
          Array.set(primitiveArray, i, innerArray);
        }
      }

      return primitiveArray;
    }

    return null;
  }

  private static Object getPrimitiveArray(Object[] array) {
    Class arrayClass = array.getClass().getComponentType();
    if (!arrayClass.isPrimitive()) {
      Class primitiveClass = getPrimitiveType(arrayClass);

      int arrayLength = array.length;
      Object primitiveArray = Array.newInstance(primitiveClass, arrayLength);

      for (int i = 0; i < arrayLength; i++) {
        Object obj = array[i];
        if (obj != null) {
          Array.set(primitiveArray, i, obj);
        }
      }
      return primitiveArray;
    }
    return array;
  }

  private static Class getPrimitiveType(Class type) {
    if (!type.isPrimitive()) {
      if (type.equals(Boolean.class)) {
        return boolean.class;
      } else if (type.equals(Integer.class)) {
        return int.class;
      } else if (type.equals(Short.class)) {
        return short.class;
      } else if (type.equals(Long.class)) {
        return long.class;
      } else if (type.equals(Byte.class)) {
        return byte.class;
      } else if (type.equals(Float.class)) {
        return float.class;
      } else if (type.equals(Double.class)) {
        return double.class;
      } else if (type.equals(Character.class)) {
        return char.class;
      }
    }
    throw new IllegalArgumentException("The type should be a wrapped primitive");
  }

  // SERIALIZATION

  final static int NULL_ID = -1;
  final static int NULL = 0;
  final static int BOOLEAN_TRUE = 2;
  final static int BOOLEAN_FALSE = 3;
  final static int INTEGER_MINUS_1 = 4;
  final static int INTEGER_0 = 5;
  final static int INTEGER_1 = 6;
  final static int INTEGER_2 = 7;
  final static int INTEGER_3 = 8;
  final static int INTEGER_4 = 9;
  final static int INTEGER_5 = 10;
  final static int INTEGER_6 = 11;
  final static int INTEGER_7 = 12;
  final static int INTEGER_8 = 13;
  final static int INTEGER_255 = 14;
  final static int INTEGER_PACK_NEG = 15;
  final static int INTEGER_PACK = 16;
  final static int LONG_MINUS_1 = 17;
  final static int LONG_0 = 18;
  final static int LONG_1 = 19;
  final static int LONG_2 = 20;
  final static int LONG_3 = 21;
  final static int LONG_4 = 22;
  final static int LONG_5 = 23;
  final static int LONG_6 = 24;
  final static int LONG_7 = 25;
  final static int LONG_8 = 26;
  final static int LONG_PACK_NEG = 27;
  final static int LONG_PACK = 28;
  final static int LONG_255 = 29;
  final static int LONG_MINUS_MAX = 30;
  final static int SHORT_MINUS_1 = 31;
  final static int SHORT_0 = 32;
  final static int SHORT_1 = 33;
  final static int SHORT_255 = 34;
  final static int SHORT_FULL = 35;
  final static int BYTE_MINUS_1 = 36;
  final static int BYTE_0 = 37;
  final static int BYTE_1 = 38;
  final static int BYTE_FULL = 39;
  final static int CHAR = 40;
  final static int FLOAT_MINUS_1 = 41;
  final static int FLOAT_0 = 42;
  final static int FLOAT_1 = 43;
  final static int FLOAT_255 = 44;
  final static int FLOAT_SHORT = 45;
  final static int FLOAT_FULL = 46;
  final static int DOUBLE_MINUS_1 = 47;
  final static int DOUBLE_0 = 48;
  final static int DOUBLE_1 = 49;
  final static int DOUBLE_255 = 50;
  final static int DOUBLE_SHORT = 51;
  final static int DOUBLE_FULL = 52;
  final static int DOUBLE_ARRAY = 53;
  final static int BIGDECIMAL = 54;
  final static int BIGINTEGER = 55;
  final static int FLOAT_ARRAY = 56;
  final static int INTEGER_MINUS_MAX = 57;
  final static int SHORT_ARRAY = 58;
  final static int BOOLEAN_ARRAY = 59;
  final static int ARRAY_INT_B = 60;
  final static int ARRAY_INT_S = 61;
  final static int ARRAY_INT_I = 62;
  final static int ARRAY_INT_PACKED = 63;
  final static int ARRAY_LONG_B = 64;
  final static int ARRAY_LONG_S = 65;
  final static int ARRAY_LONG_I = 66;
  final static int ARRAY_LONG_L = 67;
  final static int ARRAY_LONG_PACKED = 68;
  final static int CHAR_ARRAY = 69;
  final static int BYTE_ARRAY = 70;
  final static int STRING_ARRAY = 71;
  final static int ARRAY_OBJECT = 72;
  final static int STRING_EMPTY = 101;
  final static int NOTUSED_STRING_C = 102;
  final static int STRING = 103;
  final static int ARRAY_INT_C = 104;
  final static int ARRAY_LONG_C = 105;
  final static int DOUBLE_ARRAY_C = 106;
  final static int FLOAT_ARRAY_C = 107;
  final static int CHAR_ARRAY_C = 108;
  final static int BYTE_ARRAY_C = 109;
  final static int SHORT_ARRAY_C = 110;
  final static int INT_INT_ARRAY = 111;
  final static int LONG_LONG_ARRAY = 112;
  final static int CLASS = 113;
  final static int CUSTOM = 114;
  final static String EMPTY_STRING = "";

  byte[] serialize(Object obj)
      throws IOException {
    return serialize(obj, false);
  }

  byte[] serialize(Object obj, boolean compress)
      throws IOException {
    DataInputOutput ba = new DataInputOutput();

    serialize(ba, obj, compress);

    return ba.toByteArray();
  }

  private void serialize(final DataOutput out, final Object obj)
      throws IOException {
    serialize(out, obj, false);
  }

  private void serialize(final DataOutput out, final Object obj, boolean compress)
      throws IOException {
    final Class clazz = obj != null ? obj.getClass() : null;

    if (obj == null) {
      out.write(NULL);
    } else if (clazz == Boolean.class) {
      if (((Boolean) obj).booleanValue()) {
        out.write(BOOLEAN_TRUE);
      } else {
        out.write(BOOLEAN_FALSE);
      }
    } else if (clazz == Integer.class) {
      serializeInt(out, (Integer) obj);
    } else if (clazz == Double.class) {
      serializeDouble(out, (Double) obj);
    } else if (clazz == Float.class) {
      serializeFloat(out, (Float) obj);
    } else if (clazz == Long.class) {
      serializeLong(out, (Long) obj);
    } else if (clazz == BigInteger.class) {
      serializeBigInteger(out, (BigInteger) obj);
    } else if (clazz == BigDecimal.class) {
      serializeBigDecimal(out, (BigDecimal) obj);
    } else if (clazz == Short.class) {
      serializeShort(out, (Short) obj);
    } else if (clazz == Byte.class) {
      serializeByte(out, (Byte) obj);
    } else if (clazz == Character.class) {
      serializeChar(out, (Character) obj);
    } else if (clazz == String.class) {
      serializeString(out, (String) obj);
    } else if (obj instanceof Class) {
      serializeClass(out, (Class) obj);
    } else if (obj instanceof int[]) {
      serializeIntArray(out, (int[]) obj, compress);
    } else if (obj instanceof long[]) {
      serializeLongArray(out, (long[]) obj, compress);
    } else if (obj instanceof short[]) {
      serializeShortArray(out, (short[]) obj, compress);
    } else if (obj instanceof boolean[]) {
      serializeBooleanArray(out, (boolean[]) obj);
    } else if (obj instanceof double[]) {
      serializeDoubleArray(out, (double[]) obj, compress);
    } else if (obj instanceof float[]) {
      serializeFloatArray(out, (float[]) obj, compress);
    } else if (obj instanceof char[]) {
      serializeCharArray(out, (char[]) obj, compress);
    } else if (obj instanceof byte[]) {
      serializeByteArray(out, (byte[]) obj, compress);
    } else if (obj instanceof String[]) {
      serializeStringArray(out, (String[]) obj);
    } else if (obj instanceof int[][]) {
      serializeIntIntArray(out, (int[][]) obj, compress);
    } else if (obj instanceof long[][]) {
      serializeLongLongArray(out, (long[][]) obj, compress);
    } else {
      // Custom
      Serializer serializer = serializers.getSerializer(obj.getClass());
      if (serializer != null) {
        int index = serializers.getIndex(obj.getClass());
        out.write(CUSTOM + index);
        serializer.write(out, obj);
      } else if (obj instanceof Object[]) {
        serializeObjectArray(out, (Object[]) obj);
      } else {
        throw new UnsupportedTypeException(obj);
      }
    }
  }

  private static void serializeInt(final DataOutput out, final int val)
      throws IOException {
    if (val == -1) {
      out.write(INTEGER_MINUS_1);
    } else if (val == 0) {
      out.write(INTEGER_0);
    } else if (val == 1) {
      out.write(INTEGER_1);
    } else if (val == 2) {
      out.write(INTEGER_2);
    } else if (val == 3) {
      out.write(INTEGER_3);
    } else if (val == 4) {
      out.write(INTEGER_4);
    } else if (val == 5) {
      out.write(INTEGER_5);
    } else if (val == 6) {
      out.write(INTEGER_6);
    } else if (val == 7) {
      out.write(INTEGER_7);
    } else if (val == 8) {
      out.write(INTEGER_8);
    } else if (val == Integer.MIN_VALUE) {
      out.write(INTEGER_MINUS_MAX);
    } else if (val > 0 && val < 255) {
      out.write(INTEGER_255);
      out.write(val);
    } else if (val < 0) {
      out.write(INTEGER_PACK_NEG);
      LongPacker.packInt(out, -val);
    } else {
      out.write(INTEGER_PACK);
      LongPacker.packInt(out, val);
    }
  }

  private static void serializeDouble(final DataOutput out, final double val)
      throws IOException {
    if (val == -1d) {
      out.write(DOUBLE_MINUS_1);
    } else if (val == 0d) {
      out.write(DOUBLE_0);
    } else if (val == 1d) {
      out.write(DOUBLE_1);
    } else if (val >= 0 && val <= 255 && (int) val == val) {
      out.write(DOUBLE_255);
      out.write((int) val);
    } else if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE && (short) val == val) {
      out.write(DOUBLE_SHORT);
      out.writeShort((int) val);
    } else {
      out.write(DOUBLE_FULL);
      out.writeDouble(val);
    }
  }

  private static void serializeFloat(final DataOutput out, final float val)
      throws IOException {
    if (val == -1f) {
      out.write(FLOAT_MINUS_1);
    } else if (val == 0f) {
      out.write(FLOAT_0);
    } else if (val == 1f) {
      out.write(FLOAT_1);
    } else if (val >= 0 && val <= 255 && (int) val == val) {
      out.write(FLOAT_255);
      out.write((int) val);
    } else if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE && (short) val == val) {
      out.write(FLOAT_SHORT);
      out.writeShort((int) val);
    } else {
      out.write(FLOAT_FULL);
      out.writeFloat(val);
    }
  }

  private static void serializeShort(final DataOutput out, final short val)
      throws IOException {
    if (val == -1) {
      out.write(SHORT_MINUS_1);
    } else if (val == 0) {
      out.write(SHORT_0);
    } else if (val == 1) {
      out.write(SHORT_1);
    } else if (val > 0 && val < 255) {
      out.write(SHORT_255);
      out.write(val);
    } else {
      out.write(SHORT_FULL);
      out.writeShort(val);
    }
  }

  private static void serializeByte(final DataOutput out, final byte val)
      throws IOException {
    if (val == -1) {
      out.write(BYTE_MINUS_1);
    } else if (val == 0) {
      out.write(BYTE_0);
    } else if (val == 1) {
      out.write(BYTE_1);
    } else {
      out.write(BYTE_FULL);
      out.writeByte(val);
    }
  }

  private static void serializeLong(final DataOutput out, final long val)
      throws IOException {
    if (val == -1) {
      out.write(LONG_MINUS_1);
    } else if (val == 0) {
      out.write(LONG_0);
    } else if (val == 1) {
      out.write(LONG_1);
    } else if (val == 2) {
      out.write(LONG_2);
    } else if (val == 3) {
      out.write(LONG_3);
    } else if (val == 4) {
      out.write(LONG_4);
    } else if (val == 5) {
      out.write(LONG_5);
    } else if (val == 6) {
      out.write(LONG_6);
    } else if (val == 7) {
      out.write(LONG_7);
    } else if (val == 8) {
      out.write(LONG_8);
    } else if (val == Long.MIN_VALUE) {
      out.write(LONG_MINUS_MAX);
    } else if (val > 0 && val < 255) {
      out.write(LONG_255);
      out.write((int) val);
    } else if (val < 0) {
      out.write(LONG_PACK_NEG);
      LongPacker.packLong(out, -val);
    } else {
      out.write(LONG_PACK);
      LongPacker.packLong(out, val);
    }
  }

  private static void serializeChar(final DataOutput out, final char val)
      throws IOException {
    out.write(CHAR);
    out.writeChar(val);
  }

  private static void serializeString(final DataOutput out, final String val)
      throws IOException {
    if (val.length() == 0) {
      out.write(STRING_EMPTY);
    } else {
      out.write(STRING);
      final int len = val.length();
      LongPacker.packInt(out, len);
      for (int i = 0; i < len; i++) {
        int c = (int) val.charAt(i); //TODO investigate if c could be negative here
        LongPacker.packInt(out, c);
      }
    }
  }

  private static void serializeBigInteger(final DataOutput out, final BigInteger val)
      throws IOException {
    out.write(BIGINTEGER);
    byte[] buf = val.toByteArray();
    serializeByteArray(out, buf, false);
  }

  private static void serializeBigDecimal(final DataOutput out, final BigDecimal val)
      throws IOException {
    out.write(BIGDECIMAL);
    serializeByteArray(out, val.unscaledValue().toByteArray(), false);
    LongPacker.packInt(out, val.scale());
  }

  private static void serializeClass(final DataOutput out, final Class val)
      throws IOException {
    out.write(CLASS);
    serializeString(out, val.getName());
  }

  private static void serializeBooleanArray(final DataOutput out, final boolean[] val)
      throws IOException {
    out.write(BOOLEAN_ARRAY);
    LongPacker.packInt(out, val.length);
    for (boolean s : val) {
      out.writeBoolean(s);
    }
  }

  private static void serializeShortArray(final DataOutput out, final short[] val, boolean compress)
      throws IOException {
    if (compress && val.length > 250) {
      out.write(SHORT_ARRAY_C);
      byte[] b = Snappy.compress(val);
      LongPacker.packInt(out, b.length);
      out.write(b);
    } else {
      out.write(SHORT_ARRAY);
      LongPacker.packInt(out, val.length);
      for (short s : val) {
        out.writeShort(s);
      }
    }
  }

  private static void serializeDoubleArray(final DataOutput out, final double[] val, boolean compress)
      throws IOException {
    if (compress && val.length > 250) {
      out.write(DOUBLE_ARRAY_C);
      byte[] b = Snappy.compress(val);
      LongPacker.packInt(out, b.length);
      out.write(b);
    } else {
      out.write(DOUBLE_ARRAY);
      LongPacker.packInt(out, val.length);
      for (double s : val) {
        out.writeDouble(s);
      }
    }
  }

  private static void serializeFloatArray(final DataOutput out, final float[] val, boolean compress)
      throws IOException {
    if (compress && val.length > 250) {
      out.write(FLOAT_ARRAY_C);
      byte[] b = Snappy.compress(val);
      LongPacker.packInt(out, b.length);
      out.write(b);
    } else {
      out.write(FLOAT_ARRAY);
      LongPacker.packInt(out, val.length);
      for (float s : val) {
        out.writeFloat(s);
      }
    }
  }

  private static void serializeCharArray(final DataOutput out, final char[] val, boolean compress)
      throws IOException {
    if (compress && val.length > 250) {
      out.write(CHAR_ARRAY_C);
      byte[] b = Snappy.compress(val);
      LongPacker.packInt(out, b.length);
      out.write(b);
    } else {
      out.write(CHAR_ARRAY);
      LongPacker.packInt(out, val.length);
      for (char s : val) {
        out.writeChar(s);
      }
    }
  }

  private static void serializeIntArray(final DataOutput out, final int[] val, boolean compress)
      throws IOException {
    int max = Integer.MIN_VALUE;
    int min = Integer.MAX_VALUE;
    for (int i : val) {
      max = Math.max(max, i);
      min = Math.min(min, i);
    }

    if (0 <= min && max <= 255) {
      out.write(ARRAY_INT_B);
      LongPacker.packInt(out, val.length);
      for (int i : val) {
        out.write(i);
      }
    } else if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE) {
      out.write(ARRAY_INT_S);
      LongPacker.packInt(out, val.length);
      for (int i : val) {
        out.writeShort(i);
      }
    } else if (compress && val.length > 250) {
      out.write(ARRAY_INT_C);
      byte[] b = Snappy.compress(val);
      LongPacker.packInt(out, b.length);
      out.write(b);
    } else if (min >= 0) {
      out.write(ARRAY_INT_PACKED);
      LongPacker.packInt(out, val.length);
      for (int l : val) {
        LongPacker.packInt(out, l);
      }
    } else {
      out.write(ARRAY_INT_I);
      LongPacker.packInt(out, val.length);
      for (int i : val) {
        out.writeInt(i);
      }
    }
  }

  private static void serializeIntIntArray(final DataOutput out, final int[][] val, boolean compress)
      throws IOException {
    out.write(INT_INT_ARRAY);
    LongPacker.packInt(out, val.length);

    for (int[] v : val) {
      serializeIntArray(out, v, compress);
    }
  }

  private static void serializeLongArray(final DataOutput out, final long[] val, boolean compress)
      throws IOException {
    long max = Long.MIN_VALUE;
    long min = Long.MAX_VALUE;
    for (long i : val) {
      max = Math.max(max, i);
      min = Math.min(min, i);
    }

    if (0 <= min && max <= 255) {
      out.write(ARRAY_LONG_B);
      LongPacker.packInt(out, val.length);
      for (long l : val) {
        out.write((int) l);
      }
    } else if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE) {
      out.write(ARRAY_LONG_S);
      LongPacker.packInt(out, val.length);
      for (long l : val) {
        out.writeShort((short) l);
      }
    } else if (compress && val.length > 250) {
      out.write(ARRAY_LONG_C);
      byte[] b = Snappy.compress(val);
      LongPacker.packInt(out, b.length);
      out.write(b);
    } else if (0 <= min && max <= Long.MAX_VALUE) {
      out.write(ARRAY_LONG_PACKED);
      LongPacker.packInt(out, val.length);
      for (long l : val) {
        LongPacker.packLong(out, l);
      }
    } else if (Integer.MIN_VALUE <= min && max <= Integer.MAX_VALUE) {
      out.write(ARRAY_LONG_I);
      LongPacker.packInt(out, val.length);
      for (long l : val) {
        out.writeInt((int) l);
      }
    } else {
      out.write(ARRAY_LONG_L);
      LongPacker.packInt(out, val.length);
      for (long l : val) {
        out.writeLong(l);
      }
    }
  }

  private static void serializeLongLongArray(final DataOutput out, final long[][] val, boolean compress)
      throws IOException {
    out.write(LONG_LONG_ARRAY);
    LongPacker.packInt(out, val.length);

    for (long[] v : val) {
      serializeLongArray(out, v, compress);
    }
  }

  private static void serializeByteArray(final DataOutput out, final byte[] val, boolean compress)
      throws IOException {
    if (compress && val.length > 250) {
      out.write(BYTE_ARRAY_C);
      byte[] b = Snappy.compress(val);
      LongPacker.packInt(out, b.length);
      out.write(b);
    } else {
      out.write(BYTE_ARRAY);
      LongPacker.packInt(out, val.length);
      out.write(val);
    }
  }

  private static void serializeStringArray(final DataOutput out, final String[] val)
      throws IOException {
    out.write(STRING_ARRAY);
    LongPacker.packInt(out, val.length);
    for (String s : val) {
      serializeString(out, s);
    }
  }

  private void serializeObjectArray(final DataOutput out, final Object[] val)
      throws IOException {
    out.write(ARRAY_OBJECT);
    LongPacker.packInt(out, val.length);
    for (Object o : val) {
      serialize(out, o);
    }
  }

  public Object deserialize(byte[] buf)
      throws ClassNotFoundException, IOException {
    DataInputOutput bs = new DataInputOutput(buf);
    Object ret = deserialize(bs);
    if (bs.available() != 0) {
      throw new RuntimeException("bytes left: " + bs.available());
    }

    return ret;
  }

  public Object deserialize(DataInput is)
      throws IOException, ClassNotFoundException {
    Object ret = null;

    final int head = is.readUnsignedByte();

    if (head >= CUSTOM) {
      Serializer serializer = serializers.getSerializer(head - CUSTOM);
      ret = serializer.read(is);
    } else {
      switch (head) {
        case NULL:
          break;
        case BOOLEAN_TRUE:
          ret = Boolean.TRUE;
          break;
        case BOOLEAN_FALSE:
          ret = Boolean.FALSE;
          break;
        case INTEGER_MINUS_1:
          ret = Integer.valueOf(-1);
          break;
        case INTEGER_0:
          ret = Integer.valueOf(0);
          break;
        case INTEGER_1:
          ret = Integer.valueOf(1);
          break;
        case INTEGER_2:
          ret = Integer.valueOf(2);
          break;
        case INTEGER_3:
          ret = Integer.valueOf(3);
          break;
        case INTEGER_4:
          ret = Integer.valueOf(4);
          break;
        case INTEGER_5:
          ret = Integer.valueOf(5);
          break;
        case INTEGER_6:
          ret = Integer.valueOf(6);
          break;
        case INTEGER_7:
          ret = Integer.valueOf(7);
          break;
        case INTEGER_8:
          ret = Integer.valueOf(8);
          break;
        case INTEGER_MINUS_MAX:
          ret = Integer.valueOf(Integer.MIN_VALUE);
          break;
        case INTEGER_255:
          ret = Integer.valueOf(is.readUnsignedByte());
          break;
        case INTEGER_PACK_NEG:
          ret = Integer.valueOf(-LongPacker.unpackInt(is));
          break;
        case INTEGER_PACK:
          ret = Integer.valueOf(LongPacker.unpackInt(is));
          break;
        case LONG_MINUS_1:
          ret = Long.valueOf(-1);
          break;
        case LONG_0:
          ret = Long.valueOf(0);
          break;
        case LONG_1:
          ret = Long.valueOf(1);
          break;
        case LONG_2:
          ret = Long.valueOf(2);
          break;
        case LONG_3:
          ret = Long.valueOf(3);
          break;
        case LONG_4:
          ret = Long.valueOf(4);
          break;
        case LONG_5:
          ret = Long.valueOf(5);
          break;
        case LONG_6:
          ret = Long.valueOf(6);
          break;
        case LONG_7:
          ret = Long.valueOf(7);
          break;
        case LONG_8:
          ret = Long.valueOf(8);
          break;
        case LONG_255:
          ret = Long.valueOf(is.readUnsignedByte());
          break;
        case LONG_PACK_NEG:
          ret = Long.valueOf(-LongPacker.unpackLong(is));
          break;
        case LONG_PACK:
          ret = Long.valueOf(LongPacker.unpackLong(is));
          break;
        case LONG_MINUS_MAX:
          ret = Long.valueOf(Long.MIN_VALUE);
          break;
        case SHORT_MINUS_1:
          ret = Short.valueOf((short) -1);
          break;
        case SHORT_0:
          ret = Short.valueOf((short) 0);
          break;
        case SHORT_1:
          ret = Short.valueOf((short) 1);
          break;
        case SHORT_255:
          ret = Short.valueOf((short) is.readUnsignedByte());
          break;
        case SHORT_FULL:
          ret = Short.valueOf(is.readShort());
          break;
        case BYTE_MINUS_1:
          ret = Byte.valueOf((byte) -1);
          break;
        case BYTE_0:
          ret = Byte.valueOf((byte) 0);
          break;
        case BYTE_1:
          ret = Byte.valueOf((byte) 1);
          break;
        case BYTE_FULL:
          ret = Byte.valueOf(is.readByte());
          break;
        case SHORT_ARRAY:
          ret = deserializeShortArray(is);
          break;
        case BOOLEAN_ARRAY:
          ret = deserializeBooleanArray(is);
          break;
        case DOUBLE_ARRAY:
          ret = deserializeDoubleArray(is);
          break;
        case FLOAT_ARRAY:
          ret = deserializeFloatArray(is);
          break;
        case CHAR_ARRAY:
          ret = deserializeCharArray(is);
          break;
        case SHORT_ARRAY_C:
          ret = deserializeShortCompressedArray(is);
          break;
        case DOUBLE_ARRAY_C:
          ret = deserializeDoubleCompressedArray(is);
          break;
        case FLOAT_ARRAY_C:
          ret = deserializeFloatCompressedArray(is);
          break;
        case CHAR_ARRAY_C:
          ret = deserializeCharCompressedArray(is);
          break;
        case CHAR:
          ret = Character.valueOf(is.readChar());
          break;
        case FLOAT_MINUS_1:
          ret = Float.valueOf(-1);
          break;
        case FLOAT_0:
          ret = Float.valueOf(0);
          break;
        case FLOAT_1:
          ret = Float.valueOf(1);
          break;
        case FLOAT_255:
          ret = Float.valueOf(is.readUnsignedByte());
          break;
        case FLOAT_SHORT:
          ret = Float.valueOf(is.readShort());
          break;
        case FLOAT_FULL:
          ret = Float.valueOf(is.readFloat());
          break;
        case DOUBLE_MINUS_1:
          ret = Double.valueOf(-1);
          break;
        case DOUBLE_0:
          ret = Double.valueOf(0);
          break;
        case DOUBLE_1:
          ret = Double.valueOf(1);
          break;
        case DOUBLE_255:
          ret = Double.valueOf(is.readUnsignedByte());
          break;
        case DOUBLE_SHORT:
          ret = Double.valueOf(is.readShort());
          break;
        case DOUBLE_FULL:
          ret = Double.valueOf(is.readDouble());
          break;
        case BIGINTEGER:
          ret = new BigInteger((byte[]) deserialize(is));
          break;
        case BIGDECIMAL:
          ret = new BigDecimal(new BigInteger((byte[]) deserialize(is)), LongPacker.unpackInt(is));
          break;
        case STRING:
          ret = deserializeString(is);
          break;
        case STRING_EMPTY:
          ret = EMPTY_STRING;
          break;
        case CLASS:
          ret = deserializeClass(is);
          break;
        case ARRAY_INT_B:
          ret = deserializeArrayIntB(is);
          break;
        case ARRAY_INT_S:
          ret = deserializeArrayIntS(is);
          break;
        case ARRAY_INT_I:
          ret = deserializeArrayIntI(is);
          break;
        case ARRAY_INT_C:
          ret = deserializeArrayIntCompressed(is);
          break;
        case ARRAY_INT_PACKED:
          ret = deserializeArrayIntPack(is);
          break;
        case ARRAY_LONG_B:
          ret = deserializeArrayLongB(is);
          break;
        case ARRAY_LONG_S:
          ret = deserializeArrayLongS(is);
          break;
        case ARRAY_LONG_I:
          ret = deserializeArrayLongI(is);
          break;
        case ARRAY_LONG_L:
          ret = deserializeArrayLongL(is);
          break;
        case ARRAY_LONG_C:
          ret = deserializeArrayLongCompressed(is);
          break;
        case ARRAY_LONG_PACKED:
          ret = deserializeArrayLongPack(is);
          break;
        case BYTE_ARRAY:
          ret = deserializeByteArray(is);
          break;
        case BYTE_ARRAY_C:
          ret = deserializeByteCompressedArray(is);
          break;
        case STRING_ARRAY:
          ret = deserializeStringArray(is);
          break;
        case INT_INT_ARRAY:
          ret = deserializeIntIntArray(is);
          break;
        case LONG_LONG_ARRAY:
          ret = deserializeLongLongArray(is);
          break;
        case ARRAY_OBJECT:
          ret = deserializeArrayObject(is);
          break;
        case -1:
          throw new EOFException();
      }
    }
    return ret;
  }

  private static String deserializeString(DataInput buf)
      throws IOException {
    int len = LongPacker.unpackInt(buf);
    char[] b = new char[len];
    for (int i = 0; i < len; i++) {
      b[i] = (char) LongPacker.unpackInt(buf);
    }

    return new String(b);
  }

  private static Class deserializeClass(DataInput is)
      throws IOException, ClassNotFoundException {
    is.readByte();
    String className = (String) deserializeString(is);
    Class cls = Class.forName(className);
    return cls;
  }

  private static short[] deserializeShortArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    short[] ret = new short[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readShort();
    }
    return ret;
  }

  private static short[] deserializeShortCompressedArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressShortArray(b);
  }

  private static float[] deserializeFloatArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    float[] ret = new float[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readFloat();
    }
    return ret;
  }

  private static float[] deserializeFloatCompressedArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressFloatArray(b);
  }

  private static double[] deserializeDoubleArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    double[] ret = new double[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readDouble();
    }
    return ret;
  }

  private static double[] deserializeDoubleCompressedArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressDoubleArray(b);
  }

  private static char[] deserializeCharArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    char[] ret = new char[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readChar();
    }
    return ret;
  }

  private static char[] deserializeCharCompressedArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressCharArray(b);
  }

  private static boolean[] deserializeBooleanArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    boolean[] ret = new boolean[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readBoolean();
    }
    return ret;
  }

  private static String[] deserializeStringArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    String[] ret = new String[size];
    for (int i = 0; i < size; i++) {
      final int head = is.readUnsignedByte();
      switch (head) {
        case STRING:
          ret[i] = deserializeString(is);
          break;
        case STRING_EMPTY:
          ret[i] = EMPTY_STRING;
          break;
        default:
          throw new EOFException();
      }
    }
    return ret;
  }

  private static byte[] deserializeByteArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return b;
  }

  private static byte[] deserializeByteCompressedArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompress(b);
  }

  private Object[] deserializeArrayObject(DataInput is)
      throws IOException, ClassNotFoundException {
    int size = LongPacker.unpackInt(is);

    Object[] s = (Object[]) Array.newInstance(Object.class, size);
    for (int i = 0; i < size; i++) {
      s[i] = deserialize(is);
    }
    return s;
  }

  private static long[] deserializeArrayLongL(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    long[] ret = new long[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readLong();
    }
    return ret;
  }

  private static long[] deserializeArrayLongI(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    long[] ret = new long[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readInt();
    }
    return ret;
  }

  private static long[] deserializeArrayLongS(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    long[] ret = new long[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readShort();
    }
    return ret;
  }

  private static long[] deserializeArrayLongB(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    long[] ret = new long[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readUnsignedByte();
      if (ret[i] < 0) {
        throw new EOFException();
      }
    }
    return ret;
  }

  private static long[] deserializeArrayLongCompressed(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressLongArray(b);
  }

  private static long[][] deserializeLongLongArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    long[][] res = new long[size][];
    for (int i = 0; i < size; i++) {
      final int head = is.readUnsignedByte();
      switch (head) {
        case ARRAY_LONG_B:
          res[i] = deserializeArrayLongB(is);
          break;
        case ARRAY_LONG_S:
          res[i] = deserializeArrayLongS(is);
          break;
        case ARRAY_LONG_I:
          res[i] = deserializeArrayLongI(is);
          break;
        case ARRAY_LONG_L:
          res[i] = deserializeArrayLongL(is);
          break;
        case ARRAY_LONG_C:
          res[i] = deserializeArrayLongCompressed(is);
          break;
        case ARRAY_LONG_PACKED:
          res[i] = deserializeArrayLongPack(is);
          break;
        default:
          throw new IOException("Not recognized");
      }
    }
    return res;
  }

  private static int[][] deserializeIntIntArray(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    int[][] res = new int[size][];
    for (int i = 0; i < size; i++) {
      final int head = is.readUnsignedByte();
      switch (head) {
        case ARRAY_INT_B:
          res[i] = deserializeArrayIntB(is);
          break;
        case ARRAY_INT_S:
          res[i] = deserializeArrayIntS(is);
          break;
        case ARRAY_INT_I:
          res[i] = deserializeArrayIntI(is);
          break;
        case ARRAY_INT_C:
          res[i] = deserializeArrayIntCompressed(is);
          break;
        case ARRAY_INT_PACKED:
          res[i] = deserializeArrayIntPack(is);
          break;
        default:
          throw new IOException("Not recognized");
      }
    }
    return res;
  }

  private static int[] deserializeArrayIntI(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    int[] ret = new int[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readInt();
    }
    return ret;
  }

  private static int[] deserializeArrayIntS(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    int[] ret = new int[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readShort();
    }
    return ret;
  }

  private static int[] deserializeArrayIntB(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    int[] ret = new int[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readUnsignedByte();
      if (ret[i] < 0) {
        throw new EOFException();
      }
    }
    return ret;
  }

  private static int[] deserializeArrayIntPack(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    if (size < 0) {
      throw new EOFException();
    }

    int[] ret = new int[size];
    for (int i = 0; i < size; i++) {
      ret[i] = LongPacker.unpackInt(is);
    }
    return ret;
  }

  private static int[] deserializeArrayIntCompressed(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressIntArray(b);
  }

  private static long[] deserializeArrayLongPack(DataInput is)
      throws IOException {
    int size = LongPacker.unpackInt(is);
    if (size < 0) {
      throw new EOFException();
    }

    long[] ret = new long[size];
    for (int i = 0; i < size; i++) {
      ret[i] = LongPacker.unpackLong(is);
    }
    return ret;
  }
}
