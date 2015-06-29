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

package com.linkedin.paldb.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Packing utility for non-negative
 * <code>long</code> and <code>int</code> values.
 *
 * Originally developed for Kryo by Nathan Sweet. Modified for JDBM by Jan Kotek
 */
public final class LongPacker {

  // Default constructor
  private LongPacker() {

  }

  /**
   * Pack non-negative long into output stream. It will occupy 1-10 bytes
   * depending on value (lower values occupy smaller space)
   *
   * @param os the data output
   * @param value the long value
   * @return the number of bytes written
   * @throws IOException if an error occurs with the stream
   */
  static public int packLong(DataOutput os, long value)
      throws IOException {

    if (value < 0) {
      throw new IllegalArgumentException("negative value: v=" + value);
    }

    int i = 1;
    while ((value & ~0x7FL) != 0) {
      os.write((((int) value & 0x7F) | 0x80));
      value >>>= 7;
      i++;
    }
    os.write((byte) value);
    return i;
  }

  /**
   * Pack non-negative long into byte array. It will occupy 1-10 bytes
   * depending on value (lower values occupy smaller space)
   *
   * @param ba the byte array
   * @param value the long value
   * @return the number of bytes written
   * @throws IOException if an error occurs with the stream
   */
  static public int packLong(byte[] ba, long value)
      throws IOException {

    if (value < 0) {
      throw new IllegalArgumentException("negative value: v=" + value);
    }

    int i = 1;
    while ((value & ~0x7FL) != 0) {
      ba[i - 1] = (byte) (((int) value & 0x7F) | 0x80);
      value >>>= 7;
      i++;
    }
    ba[i - 1] = (byte) value;
    return i;
  }

  /**
   * Unpack positive long value from the input stream.
   *
   * @param is The input stream.
   * @return the long value
   * @throws IOException if an error occurs with the stream
   */
  static public long unpackLong(DataInput is)
      throws IOException {

    long result = 0;
    for (int offset = 0; offset < 64; offset += 7) {
      long b = is.readUnsignedByte();
      result |= (b & 0x7F) << offset;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new Error("Malformed long.");
  }

  /**
   * Unpack positive long value from the byte array.
   *
   * @param ba byte array
   * @return the long value
   */
  static public long unpackLong(byte[] ba) {
    return unpackLong(ba, 0);
  }

  /**
   * Unpack positive long value from the byte array.
   * <p>
   * The index value indicates the index in the given byte array.

   * @param ba byte array
   * @param index index in ba
   * @return the long value
   */
  static public long unpackLong(byte[] ba, int index) {
    long result = 0;
    for (int offset = 0; offset < 64; offset += 7) {
      long b = ba[index++];
      result |= (b & 0x7F) << offset;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new Error("Malformed long.");
  }

  /**
   * Pack non-negative int into output stream. It will occupy 1-5 bytes
   * depending on value (lower values occupy smaller space)
   *
   * @param os the data output
   * @param value the value
   * @return the number of bytes written
   * @throws IOException if an error occurs with the stream
   */
  static public int packInt(DataOutput os, int value)
      throws IOException {

    if (value < 0) {
      throw new IllegalArgumentException("negative value: v=" + value);
    }

    int i = 1;
    while ((value & ~0x7F) != 0) {
      os.write(((value & 0x7F) | 0x80));
      value >>>= 7;
      i++;
    }

    os.write((byte) value);
    return i;
  }

  /**
   * Unpack positive int value from the input stream.
   *
   * @param is The input stream.
   * @return the long value
   * @throws IOException if an error occurs with the stream
   */
  static public int unpackInt(DataInput is)
      throws IOException {
    for (int offset = 0, result = 0; offset < 32; offset += 7) {
      int b = is.readUnsignedByte();
      result |= (b & 0x7F) << offset;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new Error("Malformed integer.");
  }

  /**
   * Unpack positive int value from the input byte buffer.
   *
   * @param bb The byte buffer
   * @return the long value
   * @throws IOException if an error occurs with the stream
   */
  static public int unpackInt(ByteBuffer bb)
      throws IOException {
    for (int offset = 0, result = 0; offset < 32; offset += 7) {
      int b = bb.get() & 0xffff;
      result |= (b & 0x7F) << offset;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new Error("Malformed integer.");
  }
}
