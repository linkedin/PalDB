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

import java.io.*;
import java.lang.invoke.*;
import java.lang.reflect.*;
import java.nio.*;
import java.util.Arrays;


/**
 * Input/Output utility.
 */
public final class DataInputOutput implements DataInput, DataOutput, ObjectInput, ObjectOutput {

  private int pos = 0;
  private int count = 0;
  private byte[] buf;

  public DataInputOutput() {
    buf = new byte[8];
  }

  public DataInputOutput(byte[] data) {
    buf = data;
    count = data.length;
  }

  public byte[] getBuf() {
    return buf;
  }

  public int getPos() {
    return pos;
  }

  public DataInputOutput reset() {
    pos = 0;
    count = 0;
    return this;
  }

  public DataInputOutput reset(byte[] b) {
    pos = 0;
    buf = b;
    count = b.length;
    return this;
  }

  public byte[] toByteArray() {
    byte[] d = new byte[pos];
    System.arraycopy(buf, 0, d, 0, pos);
    return d;
  }

  public static void getFromBuffers(MappedByteBuffer[] buffers, long offset, byte[] slotBuffer, int slotSize, long segmentSize) {
    int bufferIndex = (int) (offset / segmentSize);
    var buf = buffers[bufferIndex];
    var pos = (int) (offset % segmentSize);

    int remaining = remaining(buf, pos);
    if (remaining < slotSize) {
      int splitOffset = 0;
      buf.get(pos, slotBuffer, 0, remaining);
      buf = buffers[++bufferIndex];
      int bytesLeft = slotSize - remaining;
      splitOffset += remaining;
      remaining = remaining(buf, 0);

      while (remaining < bytesLeft) {
        buf.get(0, slotBuffer, splitOffset, remaining);
        buf = buffers[++bufferIndex];
        splitOffset += remaining;
        bytesLeft -= remaining;
        remaining = remaining(buf, 0);
      }

      if (remaining > 0 && bytesLeft > 0) {
        buf.get(0, slotBuffer, splitOffset, bytesLeft);
      }
    } else {
      buf.get(pos, slotBuffer, 0, slotSize);
    }
  }

  public static void putIntoBuffers(MappedByteBuffer[] buffers, long offset, byte[] slotBuffer, int slotSize, long segmentSize) {
    int bufferIndex = (int) (offset / segmentSize);
    var buf = buffers[bufferIndex];
    var pos = (int) (offset % segmentSize);

    int remaining = remaining(buf, pos);
    if (remaining < slotSize) {
      int splitOffset = 0;
      buf.put(pos, slotBuffer, 0, remaining);
      buf = buffers[++bufferIndex];
      int bytesLeft = slotSize - remaining;
      splitOffset += remaining;
      remaining = remaining(buf, 0);

      while (remaining < bytesLeft) {
        buf.put(0, slotBuffer, splitOffset, remaining);
        buf = buffers[++bufferIndex];
        splitOffset += remaining;
        bytesLeft -= remaining;
        remaining = remaining(buf, 0);
      }

      if (remaining > 0 && bytesLeft > 0) {
        buf.put(0, slotBuffer, splitOffset, bytesLeft);
      }
    } else {
      buf.put(pos, slotBuffer, 0, slotSize);
    }
  }

  public static int remaining(ByteBuffer buffer, int pos) {
    return buffer.limit() - pos;
  }

  public static void unmap(MappedByteBuffer[] buffers) {
    for (final var buffer : buffers) {
      unmap(buffer);
    }
  }

  public static void unmap(MappedByteBuffer byteBuffer) {
    if (byteBuffer==null || !byteBuffer.isDirect()) return;
    if (UNSAFE_CLASS == null) {
      throw new UnsupportedOperationException("Unsafe not supported on this platform");
    }
    try {
      CLEANER_METHOD.invoke(UNSAFE, byteBuffer);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static Class<?> resolveUnsafeClass() {
    try {
      return Class.forName("sun.misc.Unsafe");
    } catch(Exception ex) {
      // jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
      // but that method should be added if sun.misc.Unsafe is removed.
      try {
        return Class.forName("jdk.internal.misc.Unsafe");
      } catch (ClassNotFoundException e) {
        return null;
      }
    }
  }

  private static final Class<?> UNSAFE_CLASS = resolveUnsafeClass();
  private static final Method CLEANER_METHOD = resolveCleaner();
  private static final Object UNSAFE = resolveUnsafe();

  private static Object resolveUnsafe() {
    if (UNSAFE_CLASS == null) throw new UnsupportedOperationException("Unsafe not supported on this platform");
    try {
      var theUnsafeField = UNSAFE_CLASS.getDeclaredField("theUnsafe");
      theUnsafeField.setAccessible(true);
      return theUnsafeField.get(null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static Method resolveCleaner() {
    if (UNSAFE_CLASS == null) throw new UnsupportedOperationException("Unsafe not supported on this platform");
    try {
      var clean = UNSAFE_CLASS.getMethod("invokeCleaner", ByteBuffer.class);
      clean.setAccessible(true);
      return clean;
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int available() {
    return count - pos;
  }

  @Override
  public void readFully(byte[] b) {
    readFully(b, 0, b.length);
  }

  @Override
  public void readFully(byte[] b, int off, int len) {
    System.arraycopy(buf, pos, b, off, len);
    pos += len;
  }

  @Override
  public int skipBytes(int n) {
    pos += n;
    return n;
  }

  @Override
  public boolean readBoolean() {
    return buf[pos++] == 1;
  }

  @Override
  public byte readByte() {
    return buf[pos++];
  }

  @Override
  public int readUnsignedByte() {
    return buf[pos++] & 0xff;
  }

  @Override
  public short readShort() {
    return (short) (((short) (buf[pos++] & 0xff) << 8) | ((short) (buf[pos++] & 0xff)));
  }

  @Override
  public int readUnsignedShort() {
    return (((int) (buf[pos++] & 0xff) << 8) | ((int) (buf[pos++] & 0xff)));
  }

  @Override
  public char readChar() {
    return (char) readInt();
  }

  @Override
  public int readInt() {
    int result = (int)INT_HANDLE.get(buf, pos);
    pos += Integer.BYTES;
    return result;
  }

  private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  @Override
  public long readLong() {
    long result = (long)LONG_HANDLE.get(buf, pos);
    pos += Long.BYTES;
    return result;
  }

  @Override
  public float readFloat() {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble() {
    return Double.longBitsToDouble(readLong());
  }

  @Override
  public String readLine()
      throws IOException {
    return readUTF();
  }

  @Override
  public String readUTF()
      throws IOException {
    int len = LongPacker.unpackInt(this);
    char[] b = new char[len];
    for (int i = 0; i < len; i++) {
      b[i] = (char) LongPacker.unpackInt(this);
    }

    return new String(b);
  }

  /**
   * make sure there will be enought space in buffer to write N bytes
   */
  private void ensureAvail(int n) {
    if (pos + n >= buf.length) {
      int newSize = Math.max(pos + n, buf.length * 2);
      buf = Arrays.copyOf(buf, newSize);
    }
  }

  @Override
  public void write(int b) {
    ensureAvail(1);
    buf[pos++] = (byte) b;
  }

  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    ensureAvail(len);
    System.arraycopy(b, off, buf, pos, len);
    pos += len;
  }

  @Override
  public void writeBoolean(boolean v) {
    ensureAvail(1);
    buf[pos++] = (byte) (v ? 1 : 0);
  }

  @Override
  public void writeByte(int v) {
    ensureAvail(1);
    buf[pos++] = (byte) (v);
  }

  @Override
  public void writeShort(int v) {
    ensureAvail(2);
    buf[pos++] = (byte) (0xff & (v >> 8));
    buf[pos++] = (byte) (0xff & (v));
  }

  @Override
  public void writeChar(int v) {
    writeInt(v);
  }

  @Override
  public void writeInt(int v) {
    ensureAvail(4);
    INT_HANDLE.set(buf, pos, v);
    pos += Integer.BYTES;
  }

  @Override
  public void writeLong(long v) {
    ensureAvail(8);
    LONG_HANDLE.set(buf, pos, v);
    pos += Long.BYTES;
  }

  @Override
  public void writeFloat(float v) {
    ensureAvail(4);
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public void writeDouble(double v) {
    ensureAvail(8);
    writeLong(Double.doubleToLongBits(v));
  }

  @Override
  public void writeBytes(String s)
      throws IOException {
    writeUTF(s);
  }

  @Override
  public void writeChars(String s)
      throws IOException {
    writeUTF(s);
  }

  @Override
  public void writeUTF(String s)
      throws IOException {
    final int len = s.length();
    LongPacker.packInt(this, len);
    for (int i = 0; i < len; i++) {
      int c = (int) s.charAt(i); //TODO investigate if c could be negative here
      LongPacker.packInt(this, c);
    }
  }

  @Override
  public int read() {
    //is here just to implement ObjectInput
    return readUnsignedByte();
  }

  @Override
  public int read(byte[] b) {
    //is here just to implement ObjectInput
    readFully(b);
    return b.length;
  }

  @Override
  public int read(byte[] b, int off, int len) {
    //is here just to implement ObjectInput
    readFully(b, off, len);
    return len;
  }

  @Override
  public long skip(long n) {
    //is here just to implement ObjectInput
    pos += n;
    return n;
  }

  @Override
  public void close() {
    //is here just to implement ObjectInput
    //do nothing
  }

  @Override
  public void flush() {
    //is here just to implement ObjectOutput
    //do nothing
  }

  @Override
  public Object readObject() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void writeObject(Object o) {
    throw new UnsupportedOperationException("Not supported");
  }
}
