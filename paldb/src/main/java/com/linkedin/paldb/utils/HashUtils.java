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

import java.util.zip.Checksum;


/**
 * Hashing utility.
 */
public class HashUtils {

  // Hash implementation
  private final Murmur3A hash = new Murmur3A(42);

  /**
   * Returns the positive hash for the given <code>bytes</code>.
   *
   * @param bytes bytes to hash
   * @return hash
   */
  public int hash(byte[] bytes) {
    hash.reset();
    hash.update(bytes);
    return hash.getIntValue() & 0x7fffffff;
  }

  /**
   * Hash implementation, inspired from java-common.
   *
   * Originally developed for greenrobot by Markus Junginger.
   */
  private static class Murmur3A implements Checksum {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private final int seed;

    private int h1;
    private int length;

    private int partialK1;
    private int partialK1Pos;

    public Murmur3A(int seed) {
      this.seed = seed;
      h1 = seed;
    }

    @Override
    public void update(int b) {
      switch (partialK1Pos) {
        case 0:
          partialK1 = 0xff & b;
          partialK1Pos = 1;
          break;
        case 1:
          partialK1 |= (0xff & b) << 8;
          partialK1Pos = 2;
          break;
        case 2:
          partialK1 |= (0xff & b) << 16;
          partialK1Pos = 3;
          break;
        case 3:
          partialK1 |= (0xff & b) << 24;
          applyK1(partialK1);
          partialK1Pos = 0;
          break;
      }
      length++;
    }

    @Override
    public void update(byte[] b, int off, int len) {
      while (partialK1Pos != 0 && len > 0) {
        update(b[off]);
        off++;
        len--;
      }

      int remainder = len & 3;
      int stop = off + len - remainder;
      for (int i = off; i < stop; i += 4) {
        int k1 = getIntLE(b, i);
        applyK1(k1);
      }
      length += stop - off;

      for (int i = 0; i < remainder; i++) {
        update(b[stop + i]);
      }
    }

    public void update(byte[] b) {
      update(b, 0, b.length);
    }

    private void applyK1(int k1) {
      k1 *= C1;
      k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
      k1 *= C2;

      h1 ^= k1;
      h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
      h1 = h1 * 5 + 0xe6546b64;
    }

    @Override
    public long getValue() {
      return 0xFFFFFFFFL & getIntValue();
    }

    public int getIntValue() {
      int finished = h1;
      if (partialK1Pos > 0) {
        int k1 = partialK1 * C1;
        k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
        k1 *= C2;
        finished ^= k1;
      }
      finished ^= length;

      // fmix
      finished ^= finished >>> 16;
      finished *= 0x85ebca6b;
      finished ^= finished >>> 13;
      finished *= 0xc2b2ae35;
      finished ^= finished >>> 16;

      return finished;
    }

    @Override
    public void reset() {
      h1 = seed;
      length = 0;
      partialK1Pos = 0;
    }

    private int getIntLE(byte[] bytes, int index) {
      return (bytes[index] & 0xff) | ((bytes[index + 1] & 0xff) << 8) |
          ((bytes[index + 2] & 0xff) << 16) | (bytes[index + 3] << 24);
    }
  }
}
