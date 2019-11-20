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

import java.lang.invoke.*;
import java.nio.ByteOrder;


/**
 * Hashing utility.
 */
public final class Murmur3 {

  private Murmur3() { }

  /**
   * Returns the positive hash for the given <code>bytes</code>.
   *
   * @param bytes bytes to hash
   * @return hash
   */
  public static long hash(byte[] bytes) {
    return hash32(bytes) & 0x7fffffff;
  }

  public static int hash(byte[] bytes, int seed) {
    return hash32(bytes, bytes.length, seed);
  }

  // Constants for 32 bit variant
  private static final int C1_32 = 0xcc9e2d51;
  private static final int C2_32 = 0x1b873593;
  private static final int R1_32 = 15;
  private static final int R2_32 = 13;
  private static final int M_32 = 5;
  private static final int N_32 = 0xe6546b64;

  public static final int DEFAULT_SEED = 104729;

  //** MurMur3 **
  /**
   * Generates 32 bit hash from byte array with the default seed.
   *
   * @param data - input byte array
   * @return 32 bit hash
   */
  public static int hash32(final byte[] data) {
    return hash32(data, 0, data.length, DEFAULT_SEED);
  }

  /**
   * Generates 32 bit hash from byte array with the given length and seed.
   *
   * @param data   - input byte array
   * @param length - length of array
   * @param seed   - seed. (default 0)
   * @return 32 bit hash
   */
  public static int hash32(final byte[] data, final int length, final int seed) {
    return hash32(data, 0, length, seed);
  }

  private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

  private static int getIntLE(byte[] array, int offset) {
    return (int)INT_HANDLE.get(array, offset);
  }

  /**
   * Generates 32 bit hash from byte array with the given length, offset and seed.
   *
   * @param data   - input byte array
   * @param offset - offset of data
   * @param length - length of array
   * @param seed   - seed. (default 0)
   * @return 32 bit hash
   */
  public static int hash32(final byte[] data, final int offset, final int length, final int seed) {
    int hash = seed;
    final int nblocks = length >> 2;

    // body
    for (int i = 0; i < nblocks; i++) {
      final int i_4 = i << 2;
      final int k = getIntLE(data, offset + i_4);
      hash = mix32(k, hash);
    }

    // tail
    final int idx = nblocks << 2;
    int k1 = 0;
    switch (length - idx) {
      case 3:
        k1 ^= data[offset + idx + 2] << 16;
      case 2:
        k1 ^= data[offset + idx + 1] << 8;
      case 1:
        k1 ^= data[offset + idx];

        // mix functions
        k1 *= C1_32;
        k1 = Integer.rotateLeft(k1, R1_32);
        k1 *= C2_32;
        hash ^= k1;
    }

    return fmix32(length, hash);
  }

  private static int mix32(int k, int hash) {
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    return Integer.rotateLeft(hash, R2_32) * M_32 + N_32;
  }

  private static int fmix32(final int length, int hash) {
    hash ^= length;
    hash ^= (hash >>> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >>> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >>> 16);

    return hash;
  }
}
