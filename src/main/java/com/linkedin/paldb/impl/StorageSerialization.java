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
import com.linkedin.paldb.utils.*;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.lang.reflect.Array;
import java.math.*;

/**
 * Internal serialization implementation.
 */
final class StorageSerialization {

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
  StorageSerialization(Configuration config) {
    this.compression = config.getBoolean(Configuration.COMPRESSION_ENABLED);
    this.serializers = config.getSerializers();
  }

  /**
   * Serializes the key object and returns it as a byte array.
   *
   * @param key key to serialize
   * @param <K> key type
   * @return key as byte array
   * @throws IOException if an io error occurs
   */
  <K> byte[] serializeKey(K key) throws IOException {
    if (key == null) {
      throw new NullPointerException();
    }
    var dataIO = new DataInputOutput();
    serializeObject(key, dataIO, false);
    return dataIO.toByteArray();
  }

  /**
   * Serializes the key and writes it into <code>dataOutput</code>.
   *
   * @param key key to serialize
   * @param dataOutput data output
   * @throws IOException if an io error occurs
   */
  void serializeKey(Object key, DataOutput dataOutput) throws IOException {
    serializeObject(key, dataOutput, false);
  }

  /**
   * Serializes the value object and returns it as a byte array.
   *
   * @param value value to serialize
   * @return value as byte array
   * @throws IOException if an io error occurs
   */
  byte[] serializeValue(Object value) throws IOException {

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
  void serializeValue(Object value, DataOutput dataOutput) throws IOException {
    serializeObject(value, dataOutput, compression);
  }

  /**
   * Serialization implementation.
   *
   * @param obj object to serialize
   * @param useCompression use compression
   * @throws IOException if an io error occurs
   */
  private void serializeObject(Object obj, DataOutput dataOutput, boolean useCompression) throws IOException {
    //Cast to primitive arrays if necessary
    if (obj != null && obj.getClass().isArray()) {
      if (obj instanceof Integer[]) {
        obj = getPrimitiveArray((Integer[]) obj);
      } else if (obj instanceof Boolean[]) {
        obj = getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Byte[]) {
        obj = getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Character[]) {
        obj = getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Double[]) {
        obj = getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Float[]) {
        obj = getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Long[]) {
        obj = getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Short[]) {
        obj = getPrimitiveArray((Object[]) obj);
      } else if (obj instanceof Integer[][]) {
        obj = getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Boolean[][]) {
        obj = getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Byte[][]) {
        obj = getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Character[][]) {
        obj = getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Double[][]) {
        obj = getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Float[][]) {
        obj = getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Long[][]) {
        obj = getPrimitiveArray((Object[][]) obj);
      } else if (obj instanceof Short[][]) {
        obj = getPrimitiveArray((Object[][]) obj);
      }
    }

    serialize(dataOutput, obj, useCompression);
  }

  /**
   * Returns true if compression is enabled.
   *
   * @return true if enabled, false otherwise
   */
  boolean isCompressionEnabled() {
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

  private static <T> Object getPrimitiveArray(T[] array) {
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

  static final int NULL_ID = -1;
  private static final int NULL = 0;
  private static final int BOOLEAN_TRUE = 2;
  private static final int BOOLEAN_FALSE = 3;
  private static final int INTEGER_MINUS_1 = 4;
  private static final int INTEGER_0 = 5;
  private static final int INTEGER_1 = 6;
  private static final int INTEGER_2 = 7;
  private static final int INTEGER_3 = 8;
  private static final int INTEGER_4 = 9;
  private static final int INTEGER_5 = 10;
  private static final int INTEGER_6 = 11;
  private static final int INTEGER_7 = 12;
  private static final int INTEGER_8 = 13;
  private static final int INTEGER_255 = 14;
  private static final int INTEGER_PACK_NEG = 15;
  private static final int INTEGER_PACK = 16;
  private static final int LONG_MINUS_1 = 17;
  private static final int LONG_0 = 18;
  private static final int LONG_1 = 19;
  private static final int LONG_2 = 20;
  private static final int LONG_3 = 21;
  private static final int LONG_4 = 22;
  private static final int LONG_5 = 23;
  private static final int LONG_6 = 24;
  private static final int LONG_7 = 25;
  private static final int LONG_8 = 26;
  private static final int LONG_PACK_NEG = 27;
  private static final int LONG_PACK = 28;
  private static final int LONG_255 = 29;
  private static final int LONG_MINUS_MAX = 30;
  private static final int SHORT_MINUS_1 = 31;
  private static final int SHORT_0 = 32;
  private static final int SHORT_1 = 33;
  private static final int SHORT_255 = 34;
  private static final int SHORT_FULL = 35;
  private static final int BYTE_MINUS_1 = 36;
  private static final int BYTE_0 = 37;
  private static final int BYTE_1 = 38;
  private static final int BYTE_FULL = 39;
  private static final int CHAR = 40;
  private static final int FLOAT_MINUS_1 = 41;
  private static final int FLOAT_0 = 42;
  private static final int FLOAT_1 = 43;
  private static final int FLOAT_255 = 44;
  private static final int FLOAT_SHORT = 45;
  private static final int FLOAT_FULL = 46;
  private static final int DOUBLE_MINUS_1 = 47;
  private static final int DOUBLE_0 = 48;
  private static final int DOUBLE_1 = 49;
  private static final int DOUBLE_255 = 50;
  private static final int DOUBLE_SHORT = 51;
  private static final int DOUBLE_FULL = 52;
  private static final int DOUBLE_ARRAY = 53;
  private static final int BIGDECIMAL = 54;
  private static final int BIGINTEGER = 55;
  private static final int FLOAT_ARRAY = 56;
  private static final int INTEGER_MINUS_MAX = 57;
  private static final int SHORT_ARRAY = 58;
  private static final int BOOLEAN_ARRAY = 59;
  private static final int ARRAY_INT_B = 60;
  private static final int ARRAY_INT_S = 61;
  private static final int ARRAY_INT_I = 62;
  private static final int ARRAY_INT_PACKED = 63;
  private static final int ARRAY_LONG_B = 64;
  private static final int ARRAY_LONG_S = 65;
  private static final int ARRAY_LONG_I = 66;
  private static final int ARRAY_LONG_L = 67;
  private static final int ARRAY_LONG_PACKED = 68;
  private static final int CHAR_ARRAY = 69;
  private static final int BYTE_ARRAY = 70;
  private static final int STRING_ARRAY = 71;
  private static final int ARRAY_OBJECT = 72;
  private static final int STRING_EMPTY = 101;
  static final int NOTUSED_STRING_C = 102;
  private static final int STRING = 103;
  private static final int ARRAY_INT_C = 104;
  private static final int ARRAY_LONG_C = 105;
  private static final int DOUBLE_ARRAY_C = 106;
  private static final int FLOAT_ARRAY_C = 107;
  private static final int CHAR_ARRAY_C = 108;
  private static final int BYTE_ARRAY_C = 109;
  private static final int SHORT_ARRAY_C = 110;
  private static final int INT_INT_ARRAY = 111;
  private static final int LONG_LONG_ARRAY = 112;
  private static final int CLASS = 113;
  private static final int CUSTOM = 114;
  private static final String EMPTY_STRING = "";

  byte[] serialize(Object obj) throws IOException {
    return serialize(obj, false);
  }

  byte[] serialize(Object obj, boolean compress) throws IOException {
    DataInputOutput ba = new DataInputOutput();

    serialize(ba, obj, compress);

    return ba.toByteArray();
  }

  private void serialize(final DataOutput out, final Object obj) throws IOException {
    serialize(out, obj, false);
  }

  private void serialize(final DataOutput out, final Object obj, boolean compress) throws IOException {
    final Class clazz = obj != null ? obj.getClass() : null;

    if (obj == null) {
      out.write(NULL);
    } else if (clazz == Boolean.class) {
      if ((boolean) obj) {
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
      var serializer = serializers.getSerializer(obj.getClass());
      if (serializer != null) {
        var className = serializer.serializedClass().getName();
        out.write(CUSTOM);
        out.writeChars(className);
        serializer.write(out, obj);
      } else if (obj instanceof Object[]) {
        serializeObjectArray(out, (Object[]) obj);
      } else {
        throw new UnsupportedTypeException(obj);
      }
    }
  }

  private static void serializeInt(final DataOutput out, final int val) throws IOException {
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

  private static void serializeDouble(final DataOutput out, final double val) throws IOException {
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

  private static void serializeFloat(final DataOutput out, final float val) throws IOException {
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

  private static void serializeShort(final DataOutput out, final short val) throws IOException {
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

  private static void serializeByte(final DataOutput out, final byte val) throws IOException {
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

  private static void serializeLong(final DataOutput out, final long val) throws IOException {
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

  private static void serializeChar(final DataOutput out, final char val) throws IOException {
    out.write(CHAR);
    out.writeChar(val);
  }

  private static void serializeString(final DataOutput out, final String val) throws IOException {
    if (val.length() == 0) {
      out.write(STRING_EMPTY);
    } else {
      out.write(STRING);
      final int len = val.length();
      LongPacker.packInt(out, len);
      for (int i = 0; i < len; i++) {
        int c = val.charAt(i); //TODO investigate if c could be negative here
        LongPacker.packInt(out, c);
      }
    }
  }

  private static void serializeBigInteger(final DataOutput out, final BigInteger val) throws IOException {
    out.write(BIGINTEGER);
    byte[] buf = val.toByteArray();
    serializeByteArray(out, buf, false);
  }

  private static void serializeBigDecimal(final DataOutput out, final BigDecimal val) throws IOException {
    out.write(BIGDECIMAL);
    serializeByteArray(out, val.unscaledValue().toByteArray(), false);
    LongPacker.packInt(out, val.scale());
  }

  private static void serializeClass(final DataOutput out, final Class val) throws IOException {
    out.write(CLASS);
    serializeString(out, val.getName());
  }

  private static void serializeBooleanArray(final DataOutput out, final boolean[] val) throws IOException {
    out.write(BOOLEAN_ARRAY);
    LongPacker.packInt(out, val.length);
    for (boolean s : val) {
      out.writeBoolean(s);
    }
  }

  private static void serializeShortArray(final DataOutput out, final short[] val, boolean compress) throws IOException {
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

  private static void serializeDoubleArray(final DataOutput out, final double[] val, boolean compress) throws IOException {
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

  private static void serializeFloatArray(final DataOutput out, final float[] val, boolean compress) throws IOException {
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

  private static void serializeCharArray(final DataOutput out, final char[] val, boolean compress) throws IOException {
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

  private static void serializeIntArray(final DataOutput out, final int[] val, boolean compress) throws IOException {
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

  private static void serializeIntIntArray(final DataOutput out, final int[][] val, boolean compress) throws IOException {
    out.write(INT_INT_ARRAY);
    LongPacker.packInt(out, val.length);

    for (int[] v : val) {
      serializeIntArray(out, v, compress);
    }
  }

  private static void serializeLongArray(final DataOutput out, final long[] val, boolean compress) throws IOException {
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

  private static void serializeLongLongArray(final DataOutput out, final long[][] val, boolean compress) throws IOException {
    out.write(LONG_LONG_ARRAY);
    LongPacker.packInt(out, val.length);

    for (long[] v : val) {
      serializeLongArray(out, v, compress);
    }
  }

  private static void serializeByteArray(final DataOutput out, final byte[] val, boolean compress) throws IOException {
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

  private static void serializeStringArray(final DataOutput out, final String[] val) throws IOException {
    out.write(STRING_ARRAY);
    LongPacker.packInt(out, val.length);
    for (String s : val) {
      serializeString(out, s);
    }
  }

  private void serializeObjectArray(final DataOutput out, final Object[] val) throws IOException {
    out.write(ARRAY_OBJECT);
    LongPacker.packInt(out, val.length);
    for (Object o : val) {
      serialize(out, o);
    }
  }

  Object deserialize(byte[] buf) throws IOException {
    DataInputOutput bs = new DataInputOutput(buf);
    Object ret = deserialize(bs);
    if (bs.available() != 0) {
      throw new RuntimeException("bytes left: " + bs.available());
    }

    return ret;
  }

  Object deserialize(DataInput is) throws IOException {
    Object ret = null;

    final int head = is.readUnsignedByte();

    if (head >= CUSTOM) {
      var className = is.readUTF();
      Serializer serializer = serializers.getSerializer(className);
      if (serializer == null) {
        throw new MissingSerializer("Serializer not registered: " + className);
      }
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
          ret = -1;
          break;
        case INTEGER_0:
          ret = 0;
          break;
        case INTEGER_1:
          ret = 1;
          break;
        case INTEGER_2:
          ret = 2;
          break;
        case INTEGER_3:
          ret = 3;
          break;
        case INTEGER_4:
          ret = 4;
          break;
        case INTEGER_5:
          ret = 5;
          break;
        case INTEGER_6:
          ret = 6;
          break;
        case INTEGER_7:
          ret = 7;
          break;
        case INTEGER_8:
          ret = 8;
          break;
        case INTEGER_MINUS_MAX:
          ret = Integer.MIN_VALUE;
          break;
        case INTEGER_255:
          ret = is.readUnsignedByte();
          break;
        case INTEGER_PACK_NEG:
          ret = -LongPacker.unpackInt(is);
          break;
        case INTEGER_PACK:
          ret = LongPacker.unpackInt(is);
          break;
        case LONG_MINUS_1:
          ret = (long) -1;
          break;
        case LONG_0:
          ret = 0L;
          break;
        case LONG_1:
          ret = 1L;
          break;
        case LONG_2:
          ret = 2L;
          break;
        case LONG_3:
          ret = 3L;
          break;
        case LONG_4:
          ret = 4L;
          break;
        case LONG_5:
          ret = 5L;
          break;
        case LONG_6:
          ret = 6L;
          break;
        case LONG_7:
          ret = 7L;
          break;
        case LONG_8:
          ret = 8L;
          break;
        case LONG_255:
          ret = (long) is.readUnsignedByte();
          break;
        case LONG_PACK_NEG:
          ret = -LongPacker.unpackLong(is);
          break;
        case LONG_PACK:
          ret = LongPacker.unpackLong(is);
          break;
        case LONG_MINUS_MAX:
          ret = Long.MIN_VALUE;
          break;
        case SHORT_MINUS_1:
          ret = (short) -1;
          break;
        case SHORT_0:
          ret = (short) 0;
          break;
        case SHORT_1:
          ret = (short) 1;
          break;
        case SHORT_255:
          ret = (short) is.readUnsignedByte();
          break;
        case SHORT_FULL:
          ret = is.readShort();
          break;
        case BYTE_MINUS_1:
          ret = (byte) -1;
          break;
        case BYTE_0:
          ret = (byte) 0;
          break;
        case BYTE_1:
          ret = (byte) 1;
          break;
        case BYTE_FULL:
          ret = is.readByte();
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
          ret = is.readChar();
          break;
        case FLOAT_MINUS_1:
          ret = (float) -1;
          break;
        case FLOAT_0:
          ret = (float) 0;
          break;
        case FLOAT_1:
          ret = 1f;
          break;
        case FLOAT_255:
          ret = (float) is.readUnsignedByte();
          break;
        case FLOAT_SHORT:
          ret = (float) is.readShort();
          break;
        case FLOAT_FULL:
          ret = is.readFloat();
          break;
        case DOUBLE_MINUS_1:
          ret = (double) -1;
          break;
        case DOUBLE_0:
          ret = (double) 0;
          break;
        case DOUBLE_1:
          ret = 1d;
          break;
        case DOUBLE_255:
          ret = (double) is.readUnsignedByte();
          break;
        case DOUBLE_SHORT:
          ret = (double) is.readShort();
          break;
        case DOUBLE_FULL:
          ret = is.readDouble();
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

  private static Class deserializeClass(DataInput is) throws IOException {
    is.readByte();
    String className = deserializeString(is);
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new MissingClass("Class is missing: " + className);
    }
  }

  private static short[] deserializeShortArray(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    short[] ret = new short[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readShort();
    }
    return ret;
  }

  private static short[] deserializeShortCompressedArray(DataInput is) throws IOException {
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

  private static float[] deserializeFloatCompressedArray(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressFloatArray(b);
  }

  private static double[] deserializeDoubleArray(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    double[] ret = new double[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readDouble();
    }
    return ret;
  }

  private static double[] deserializeDoubleCompressedArray(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressDoubleArray(b);
  }

  private static char[] deserializeCharArray(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    char[] ret = new char[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readChar();
    }
    return ret;
  }

  private static char[] deserializeCharCompressedArray(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressCharArray(b);
  }

  private static boolean[] deserializeBooleanArray(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    boolean[] ret = new boolean[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readBoolean();
    }
    return ret;
  }

  private static String[] deserializeStringArray(DataInput is) throws IOException {
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

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static byte[] deserializeByteArray(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    if (size == 0) {
      return EMPTY_BYTES;
    }
    byte[] b = new byte[size];
    is.readFully(b);
    return b;
  }

  private static byte[] deserializeByteCompressedArray(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompress(b);
  }

  private Object[] deserializeArrayObject(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);

    Object[] s = (Object[]) Array.newInstance(Object.class, size);
    for (int i = 0; i < size; i++) {
      s[i] = deserialize(is);
    }
    return s;
  }

  private static long[] deserializeArrayLongL(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    long[] ret = new long[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readLong();
    }
    return ret;
  }

  private static long[] deserializeArrayLongI(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    long[] ret = new long[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readInt();
    }
    return ret;
  }

  private static long[] deserializeArrayLongS(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    long[] ret = new long[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readShort();
    }
    return ret;
  }

  private static long[] deserializeArrayLongB(DataInput is) throws IOException {
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

  private static long[] deserializeArrayLongCompressed(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressLongArray(b);
  }

  private static long[][] deserializeLongLongArray(DataInput is) throws IOException {
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

  private static int[][] deserializeIntIntArray(DataInput is) throws IOException {
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

  private static int[] deserializeArrayIntI(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    int[] ret = new int[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readInt();
    }
    return ret;
  }

  private static int[] deserializeArrayIntS(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    int[] ret = new int[size];
    for (int i = 0; i < size; i++) {
      ret[i] = is.readShort();
    }
    return ret;
  }

  private static int[] deserializeArrayIntB(DataInput is) throws IOException {
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

  private static int[] deserializeArrayIntPack(DataInput is) throws IOException {
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

  private static int[] deserializeArrayIntCompressed(DataInput is) throws IOException {
    int size = LongPacker.unpackInt(is);
    byte[] b = new byte[size];
    is.readFully(b);
    return Snappy.uncompressIntArray(b);
  }

  private static long[] deserializeArrayLongPack(DataInput is) throws IOException {
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
