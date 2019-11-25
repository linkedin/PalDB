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


class TestLongPacker {

  @Test
  void testPackInt()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packInt(dio.reset(), 42);
    assertEquals(42, LongPacker.unpackInt(dio.reset(dio.toByteArray())));
  }

  @Test
  void testPackIntZero()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packInt(dio.reset(), 0);
    assertEquals(0, LongPacker.unpackInt(dio.reset(dio.toByteArray())));
  }

  @Test
  void testPackIntMax()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packInt(dio.reset(), Integer.MAX_VALUE);
    assertEquals(Integer.MAX_VALUE, LongPacker.unpackInt(dio.reset(dio.toByteArray())));
  }

  @Test
  void testPackIntNeg() {
    DataInputOutput dio = new DataInputOutput();
    assertThrows(IllegalArgumentException.class, () -> LongPacker.packInt(dio.reset(), -42));
  }

  @Test
  void testPackLong()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packLong(dio.reset(), 42L);
    assertEquals(42, LongPacker.unpackLong(dio.reset(dio.toByteArray())));
  }

  @Test
  void testPackLongZero()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packLong(dio.reset(), 0L);
    assertEquals(0L, LongPacker.unpackLong(dio.reset(dio.toByteArray())));
  }

  @Test
  void testPackLongBytes() {
    byte[] buf = new byte[15];
    LongPacker.packLong(buf, 42L);
    assertEquals(42L, LongPacker.unpackLong(buf));
  }

  @Test
  void testPackLongMax()
      throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packLong(dio.reset(), Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, LongPacker.unpackLong(dio.reset(dio.toByteArray())));
  }

  @Test
  void testPackLongBytesMax() {
    byte[] buf = new byte[15];
    LongPacker.packLong(buf, Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, LongPacker.unpackLong(buf));
  }

  @Test
  void testPackLongNeg() {
    DataInputOutput dio = new DataInputOutput();
    assertThrows(IllegalArgumentException.class, () -> LongPacker.packLong(dio.reset(), -42L));
  }

  @Test
  void testPackLongBytesNeg() {
    assertThrows(IllegalArgumentException.class, () -> LongPacker.packLong(new byte[15], -42L));
  }

  @Test
  void test() throws IOException {
    DataInputOutput dio = new DataInputOutput();
    LongPacker.packInt(dio.reset(), 5);
    ByteBuffer bb = ByteBuffer.wrap(dio.getBuf());
    assertEquals(5, LongPacker.unpackInt(bb));
  }
}
