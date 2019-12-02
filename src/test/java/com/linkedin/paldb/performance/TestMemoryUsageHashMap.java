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

import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.performance.utils.NanoBench;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.*;

@Disabled
@Tag("performance")
class TestMemoryUsageHashMap {

  private Set<Integer> ref;

  @Test
  void testMemoryUsage() {

    System.out.println("MEMORY USAGE (HashSet int)\n\n");
    System.out.println("KEYS;MB");

    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {

      // Benchmark
      final int cnt = i;
      NanoBench nanoBench = NanoBench.create();
      nanoBench.memoryOnly().warmUps(2).measurements(3).measure(String.format("Measure memory for %d keys", i), () -> {
        Random setRandom = new Random(4532);
        ref = new HashSet<>(cnt);
        while (ref.size() < cnt) {
          ref.add(setRandom.nextInt(Integer.MAX_VALUE));
        }
      });

      double bytes = nanoBench.getMemoryBytes() / (1024.0 * 1024.0);
      System.out.println(i + ";" + bytes);
    }
  }

  @Test
  void testMemoryUsagePalDB(@TempDir Path tempDir) {

    System.out.println("MEMORY USAGE (PalDB int)\n\n");
    System.out.println("KEYS;MB");
    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {

      // Benchmark
      final int cnt = i;
      NanoBench nanoBench = NanoBench.create();
      nanoBench.memoryOnly().warmUps(2).measurements(3).measure(String.format("Measure memory for %d keys", i), () -> {
        Random setRandom = new Random(4532);
        try (var palDB = PalDB.<Integer,Boolean>createRW(Files.createTempFile(tempDir, "test", ".paldb").toFile())) {
          try (var init = palDB.init()) {
            for (int j = 0; j < cnt; j++) {
              init.put(setRandom.nextInt(Integer.MAX_VALUE), Boolean.TRUE);
            }
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });

      double bytes = nanoBench.getMemoryBytes() / (1024.0 * 1024.0);
      System.out.println(i + ";" + bytes);
    }
  }
}
