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

package com.linkedin.paldb;

import com.linkedin.paldb.utils.NanoBench;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.testng.annotations.Test;


public class TestMemoryUsageHashMap {

  private Set<Integer> ref;

  @Test
  public void testMemoryUsage() {

    System.out.println("MEMORY USAGE (HashSet int)\n\n");
    System.out.println("KEYS;MB");

    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {

      // Benchmark
      final int cnt = i;
      NanoBench nanoBench = NanoBench.create();
      nanoBench.memoryOnly().warmUps(2).measurements(10).measure(String.format("Measure memory for %d keys", i), new Runnable() {
        @Override
        public void run() {
          Random setRandom = new Random(4532);
          ref = new HashSet<Integer>(cnt);
          while (ref.size() < cnt) {
            ref.add(setRandom.nextInt(Integer.MAX_VALUE));
          }
        }
      });

      double bytes = nanoBench.getMemoryBytes() / (1024.0 * 1024.0);
      System.out.println(i + ";" + bytes);
    }
  }
}
