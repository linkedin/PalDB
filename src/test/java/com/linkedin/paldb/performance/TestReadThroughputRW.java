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

import com.linkedin.paldb.api.*;
import com.linkedin.paldb.impl.GenerateTestData;
import com.linkedin.paldb.performance.utils.*;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Disabled
@Tag("performance")
class TestReadThroughputRW {

  private File testFolder = createTempDir();
  private static final int READS = 500000;

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @BeforeEach
  void setUp() {
    DirectoryUtils.deleteDirectory(testFolder);
    testFolder.mkdir();
  }

  @AfterEach
  void cleanUp() {
    DirectoryUtils.deleteDirectory(testFolder);
  }

  private static File createTempDir() {
    try {
      return Files.createTempDirectory("testreadthroughput").toFile();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  void testReadThroughput() {

    List<Measure> measures = new ArrayList<>();
    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {
      Measure m = measure(i, 0, 0, false, 1);

      measures.add(m);
    }

    report("READ THROUGHPUT (Set int -> boolean)", measures);
  }

  @Test
  void testReadThroughputMultiThread() {

    List<Measure> measures = new ArrayList<>();
    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {
      Measure m = measure(i, 0, 0, false, 4);

      measures.add(m);
    }

    report("READ THROUGHPUT MULTI THREAD (Set int -> boolean)", measures);
  }

  @Test
  void testReadThroughputWithCache() {

    List<Measure> measures = new ArrayList<>();
    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {
      Measure m = measure(i, 0, 0.05, false, 1);

      measures.add(m);
    }

    report("READ THROUGHPUT WITH BLOOM FILTER (Set int -> boolean)", measures);
  }

  @Test
  void testReadThroughputWithCacheRandomFinds() {

    List<Measure> measures = new ArrayList<>();
    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {
      Measure m = measure(i, 0, 0.01, true, 1);

      measures.add(m);
    }

    report("READ THROUGHPUT WITH BLOOM FILTER RANDOM FINDS (Set int -> boolean)", measures);
  }

  @Test
  void testReadThroughputWithCacheRandomFindsMultipleThreads() {

    List<Measure> measures = new ArrayList<>();
    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {
      Measure m = measure(i, 0, 0.01, true, 4);

      measures.add(m);
    }

    report("READ THROUGHPUT WITH BLOOM FILTER RANDOM FINDS MULTITHREADED (Set int -> boolean)", measures);
  }

  // UTILITY

  private Measure measure(int keysCount, int valueLength, double errorRate, boolean randomReads, int noOfThreads) {
    // Write store
    File storeFile = new File(testFolder, "paldb" + keysCount + "-" + valueLength + ".store");
    // Generate keys
    long seed = 4242;
    final Integer[] keys = GenerateTestData.generateRandomIntKeys(keysCount, Integer.MAX_VALUE, seed);

    var configBuilder = PalDBConfigBuilder.<String,String>create();
    if (errorRate > 0) {
      configBuilder.withEnableBloomFilter(true)
              .withBloomFilterErrorFactor(errorRate);
    }

    var config = configBuilder
            .build();

    try (var storeRW = PalDB.createRW(storeFile, config)) {

      try (var init = storeRW.init()) {
        for (var key : keys) {
          if (valueLength == 0) {
            init.put(key.toString(), Boolean.TRUE.toString());
          } else {
            init.put(key.toString(), RandomStringUtils.randomAlphabetic(valueLength));
          }
        }
      }

      var totalCount = new AtomicInteger(0);
      var findCount = new AtomicInteger(0);

      // Measure
      NanoBench nanoBench = NanoBench.create();
      nanoBench.cpuOnly().warmUps(5).measurements(20).measure("Measure %d reads for %d keys with cache", () -> {
        if (noOfThreads < 2) {
          doWork(randomReads, keys, totalCount, findCount, storeRW);
        } else {
          var forkJoinPool = new ForkJoinPool(noOfThreads);
          try {
            forkJoinPool.submit(() -> IntStream.range(0, noOfThreads).parallel()
                    .forEach(i -> doWork(randomReads, keys, totalCount, findCount, storeRW))
            ).join();
          } finally {
            forkJoinPool.shutdown();
          }
        }
      });

      // Return measure
      double rps = READS * noOfThreads * nanoBench.getTps();
      return new Measure(storeFile.length(), rps, findCount.get(), totalCount.get(), keys.length);
    }
  }

  private void doWork(boolean randomReads, Integer[] keys, AtomicInteger totalCount, AtomicInteger findCount,
                      StoreReader<String, String> reader) {
    Random r = new Random(42);
    int length = keys.length;
    for (int j = 0; j < READS; j++) {
      totalCount.incrementAndGet();
      int key;
      if (randomReads) {
        key = r.nextInt(Integer.MAX_VALUE);
      } else {
        key = keys[r.nextInt(length)];
      }
      var value = reader.get(Integer.toString(key));
      if (value != null) {
        findCount.incrementAndGet();
      }
    }
  }

  private void report(String title, List<Measure> measures) {
    System.out.println(title);
    System.out.println("FILE LENGTH;\tKEYS;\tRPS;\tVALUES FOUND;\tTOTAL READS");
    for (Measure m : measures) {
      System.out.println(m.fileSize + ";\t" + m.keys + ";\t" + m.rps + ";\t" + m.valueLength + ";\t" + m.cacheSize);
    }
  }

  // Measurement class
  private static class Measure {
    private long fileSize;
    private double rps;
    private int valueLength;
    private long cacheSize;
    private int keys;

    private Measure(long fileSize, double rps, int valueLength, long cacheSize, int keys) {
      this.fileSize = fileSize;
      this.rps = rps;
      this.valueLength = valueLength;
      this.cacheSize = cacheSize;
      this.keys = keys;
    }
  }
}
