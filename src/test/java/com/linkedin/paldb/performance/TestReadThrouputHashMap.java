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

package com.linkedin.paldb.performance;

import com.linkedin.paldb.performance.utils.NanoBench;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
@Tag("performance")
class TestReadThrouputHashMap {

  private final int READS = 500000;

  @Test
  void testReadThroughput() {

    System.out.println("READ THROUGHPUT (HashSet int)\n\n");
    System.out.println("KEYS;RPS");

    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {

      // Prepare set
      Random setRandom = new Random(4532);
      final Set<Integer> set = new HashSet<>(i);
      while (set.size() < i) {
        set.add(setRandom.nextInt(Integer.MAX_VALUE));
      }
      final Integer[] keys = set.toArray(new Integer[0]);

      // Benchmark
      final Random random = new Random(i);
      NanoBench nanoBench = NanoBench.create();
      nanoBench.cpuOnly().measure(String.format("Measure %d reads for %d keys", READS, i), () -> {
        for (int i1 = 0; i1 < READS; i1++) {
          assertTrue(set.contains(keys[random.nextInt(keys.length)]));
        }
      });

      double rps = READS * nanoBench.getTps();
      System.out.println(keys.length + ";" + rps);
    }
  }
}
