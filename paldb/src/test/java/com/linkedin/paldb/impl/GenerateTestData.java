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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.RandomStringUtils;


/**
 * Utility to generate test data.
 */
public class GenerateTestData {

  private final static Random rng = new Random(31);

  public static int[][] generateIntData1(int m, int n) {
    int[][] mat = new int[m][n];

    for (int i = 0; i < m; ++i) {
      for (int j = 0; j < n; ++j) {
        mat[i][j] = rng.nextInt();
      }
    }

    return mat;
  }

  public static Integer[] generateIntKeys(int count) {
    Integer[] res = new Integer[count];
    for (int i = 0; i < count; i++) {
      res[i] = i;
    }
    return res;
  }

  public static Integer[] generateRandomIntKeys(int count, long seed) {
    return generateRandomIntKeys(count, Math.min(count * 50, Integer.MAX_VALUE), seed);
  }

  public static Integer[] generateRandomIntKeys(int count, int range, long seed) {
    Random random = new Random(seed);
    Set<Integer> set = new HashSet<Integer>(count);
    while (set.size() < count) {
      set.add(random.nextInt(range));
    }
    return set.toArray(new Integer[0]);
  }

  public static String[] generateStringKeys(int count) {
    String[] res = new String[count];
    for (int i = 0; i < count; i++) {
      res[i] = i + "";
    }
    return res;
  }

  public static Byte[] generateByteKeys(int count) {
    if (count > 127) {
      throw new RuntimeException("Too large range");
    }
    Byte[] res = new Byte[count];
    for (int i = 0; i < count; i++) {
      res[i] = (byte) i;
    }
    return res;
  }

  public static Double[] generateDoubleKeys(int count) {
    Double[] res = new Double[count];
    for (int i = 0; i < count; i++) {
      res[i] = (double) i;
    }
    return res;
  }

  public static Long[] generateLongKeys(int count) {
    Long[] res = new Long[count];
    for (int i = 0; i < count; i++) {
      res[i] = (long) i;
    }
    return res;
  }

  public static Object[] generateCompoundKeys(int count) {
    Object[] res = new Object[count];
    Random random = new Random(345);
    for (int i = 0; i < count; i++) {
      Object[] k = new Object[]{new Byte((byte) random.nextInt(10)), new Integer(i)};
      res[i] = k;
    }
    return res;
  }

  public static Object[] generateCompoundByteKey() {
    Object[] res = new Object[2];
    res[0] = (byte) 6;
    res[1] = (byte) 0;
    return res;
  }

  public static String generateStringData(int letters) {
    return RandomStringUtils.randomAlphabetic(letters);
  }

  public static String[] generateStringData(int count, int letters) {
    String[] res = new String[count];
    for (int i = 0; i < count; i++) {
      res[i] = RandomStringUtils.randomAlphabetic(letters);
    }
    return res;
  }

  public static Integer[] generateIntData(int count) {
    Integer[] res = new Integer[count];
    Random random = new Random(count + 34593263544354353l);
    for (int i = 0; i < count; i++) {
      res[i] = random.nextInt(1000000);
    }
    return res;
  }

  public static int[][] generateIntArrayData(int count, int size) {
    int[][] res = new int[count][];
    Random random = new Random(count + 34593263544354353l);
    for (int i = 0; i < count; i++) {
      int[] r = new int[size];
      for (int j = 0; j < size; j++) {
        r[j] = random.nextInt(1000000);
      }
      res[i] = r;
    }
    return res;
  }
}
