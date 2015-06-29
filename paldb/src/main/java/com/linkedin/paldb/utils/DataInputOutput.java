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
import java.io.ObjectInput;
import java.io.ObjectOutput;
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

  public void resetForReading() {
    count = pos;
    pos = 0;
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

  @Override
  public int available() {
    return count - pos;
  }

  @Override
  public void readFully(byte[] b)
      throws IOException {
    readFully(b, 0, b.length);
  }

  @Override
  public void readFully(byte[] b, int off, int len)
      throws IOException {
    System.arraycopy(buf, pos, b, off, len);
    pos += len;
  }

  @Override
  public int skipBytes(int n)
      throws IOException {
    pos += n;
    return n;
  }

  @Override
  public boolean readBoolean()
      throws IOException {
    return buf[pos++] == 1;
  }

  @Override
  public byte readByte()
      throws IOException {
    return buf[pos++];
  }

  @Override
  public int readUnsignedByte()
      throws IOException {
    return buf[pos++] & 0xff;
  }

  @Override
  public short readShort()
      throws IOException {
    return (short) (((short) (buf[pos++] & 0xff) << 8) | ((short) (buf[pos++] & 0xff) << 0));
  }

  @Override
  public int readUnsignedShort()
      throws IOException {
    return (((int) (buf[pos++] & 0xff) << 8) | ((int) (buf[pos++] & 0xff) << 0));
  }

  @Override
  public char readChar()
      throws IOException {
    return (char) readInt();
  }

  @Override
  public int readInt()
      throws IOException {
    return (((buf[pos++] & 0xff) << 24) | ((buf[pos++] & 0xff) << 16) | ((buf[pos++] & 0xff) << 8) | (
        (buf[pos++] & 0xff) << 0));
  }

  @Override
  public long readLong()
      throws IOException {
    return (((long) (buf[pos++] & 0xff) << 56) | ((long) (buf[pos++] & 0xff) << 48) | ((long) (buf[pos++] & 0xff) << 40)
        | ((long) (buf[pos++] & 0xff) << 32) | ((long) (buf[pos++] & 0xff) << 24) | ((long) (buf[pos++] & 0xff) << 16)
        | ((long) (buf[pos++] & 0xff) << 8) | ((long) (buf[pos++] & 0xff) << 0));
  }

  @Override
  public float readFloat()
      throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble()
      throws IOException {
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
  public void write(int b)
      throws IOException {
    ensureAvail(1);
    buf[pos++] = (byte) b;
  }

  @Override
  public void write(byte[] b)
      throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len)
      throws IOException {
    ensureAvail(len);
    System.arraycopy(b, off, buf, pos, len);
    pos += len;
  }

  @Override
  public void writeBoolean(boolean v)
      throws IOException {
    ensureAvail(1);
    buf[pos++] = (byte) (v ? 1 : 0);
  }

  @Override
  public void writeByte(int v)
      throws IOException {
    ensureAvail(1);
    buf[pos++] = (byte) (v);
  }

  @Override
  public void writeShort(int v)
      throws IOException {
    ensureAvail(2);
    buf[pos++] = (byte) (0xff & (v >> 8));
    buf[pos++] = (byte) (0xff & (v >> 0));
  }

  @Override
  public void writeChar(int v)
      throws IOException {
    writeInt(v);
  }

  @Override
  public void writeInt(int v)
      throws IOException {
    ensureAvail(4);
    buf[pos++] = (byte) (0xff & (v >> 24));
    buf[pos++] = (byte) (0xff & (v >> 16));
    buf[pos++] = (byte) (0xff & (v >> 8));
    buf[pos++] = (byte) (0xff & (v >> 0));
  }

  @Override
  public void writeLong(long v)
      throws IOException {
    ensureAvail(8);
    buf[pos++] = (byte) (0xff & (v >> 56));
    buf[pos++] = (byte) (0xff & (v >> 48));
    buf[pos++] = (byte) (0xff & (v >> 40));
    buf[pos++] = (byte) (0xff & (v >> 32));
    buf[pos++] = (byte) (0xff & (v >> 24));
    buf[pos++] = (byte) (0xff & (v >> 16));
    buf[pos++] = (byte) (0xff & (v >> 8));
    buf[pos++] = (byte) (0xff & (v >> 0));
  }

  @Override
  public void writeFloat(float v)
      throws IOException {
    ensureAvail(4);
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public void writeDouble(double v)
      throws IOException {
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
  public int read()
      throws IOException {
    //is here just to implement ObjectInput
    return readUnsignedByte();
  }

  @Override
  public int read(byte[] b)
      throws IOException {
    //is here just to implement ObjectInput
    readFully(b);
    return b.length;
  }

  @Override
  public int read(byte[] b, int off, int len)
      throws IOException {
    //is here just to implement ObjectInput
    readFully(b, off, len);
    return len;
  }

  @Override
  public long skip(long n)
      throws IOException {
    //is here just to implement ObjectInput
    pos += n;
    return n;
  }

  @Override
  public void close()
      throws IOException {
    //is here just to implement ObjectInput
    //do nothing
  }

  @Override
  public void flush()
      throws IOException {
    //is here just to implement ObjectOutput
    //do nothing
  }

  @Override
  public Object readObject()
      throws ClassNotFoundException, IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void writeObject(Object o)
      throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }
}
