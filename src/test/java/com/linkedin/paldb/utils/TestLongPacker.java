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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;


public class TestLongPacker {

  @Test
  public void testPackInt()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packInt(dio.reset(), 42);
    assertEquals(LongPacker.unpackInt(dio.reset(dio.toByteArray())), 42);
  }

  @Test
  public void testPackIntZero()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packInt(dio.reset(), 0);
    assertEquals(LongPacker.unpackInt(dio.reset(dio.toByteArray())), 0);
  }

  @Test
  public void testPackIntMax()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packInt(dio.reset(), Integer.MAX_VALUE);
    assertEquals(LongPacker.unpackInt(dio.reset(dio.toByteArray())), Integer.MAX_VALUE);
  }

  @Test
  public void testPackIntNeg() {
    DataInputOutput dio = new DataInputOutput();
    assertThrows(IllegalArgumentException.class, () -> LongPacker.packInt(dio.reset(), -42));
  }

  @Test
  public void testPackLong()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packLong(dio.reset(), 42l);
    assertEquals(LongPacker.unpackLong(dio.reset(dio.toByteArray())), 42);
  }

  @Test
  public void testPackLongZero()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packLong(dio.reset(), 0l);
    assertEquals(LongPacker.unpackLong(dio.reset(dio.toByteArray())), 0l);
  }

  @Test
  public void testPackLongBytes()
      throws IOException {
    byte[] buf = new byte[15];
    LongPacker.packLong(buf, 42l);
    assertEquals(LongPacker.unpackLong(buf), 42l);
  }

  @Test
  public void testPackLongMax()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packLong(dio.reset(), Long.MAX_VALUE);
    assertEquals(LongPacker.unpackLong(dio.reset(dio.toByteArray())), Long.MAX_VALUE);
  }

  @Test
  public void testPackLongBytesMax()
      throws IOException {
    byte[] buf = new byte[15];
    LongPacker.packLong(buf, Long.MAX_VALUE);
    assertEquals(LongPacker.unpackLong(buf), Long.MAX_VALUE);
  }

  @Test
  public void testPackLongNeg()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    assertThrows(IllegalArgumentException.class, () -> LongPacker.packLong(dio.reset(), -42L));
  }

  @Test
  public void testPackLongBytesNeg() {
    assertThrows(IllegalArgumentException.class, () -> LongPacker.packLong(new byte[15], -42L));
  }

  @Test
  public void test() throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packInt(dio.reset(), 5);
    ByteBuffer bb = ByteBuffer.wrap(dio.getBuf());
    assertEquals(LongPacker.unpackInt(bb), 5);
  }
}
