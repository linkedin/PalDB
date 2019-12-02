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

import com.linkedin.paldb.impl.GenerateTestData;
import com.linkedin.paldb.performance.utils.*;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.api.*;
import org.rocksdb.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Disabled
@Tag("performance")
class TestReadThrouputRocksDB {

  private File TEST_FOLDER = new File("testreadthroughputrocksdb");
  private final int READS = 500000;

  @BeforeEach
  void loadLibrary() {
    RocksDB.loadLibrary();
  }

  @BeforeEach
  void setUp() {
    DirectoryUtils.deleteDirectory(TEST_FOLDER);
    TEST_FOLDER.mkdir();
  }

  @AfterEach
  void cleanUp() {
    DirectoryUtils.deleteDirectory(TEST_FOLDER);
  }

  @Test
  void testReadThroughput() throws RocksDBException {
    List<Measure> measures = new ArrayList<>();

    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {
      Measure m = measure(i, 0, 0, false, false, 1);

      measures.add(m);
    }

    report("ROCKSDB READ THROUGHPUT (Set int -> boolean)", measures);
  }

  @Test
  void testReadThroughputMultiThreaded() throws RocksDBException {
    List<Measure> measures = new ArrayList<>();

    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {
      Measure m = measure(i, 0, 0, false, false, 4);

      measures.add(m);
    }

    report("ROCKSDB READ THROUGHPUT MULTITHREADED (Set int -> boolean)", measures);
  }

  @Test
  void testRandomReadThroughputMultiThreaded() throws RocksDBException {
    List<Measure> measures = new ArrayList<>();

    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {
      Measure m = measure(i, 0, 0, false, true, 4);

      measures.add(m);
    }

    report("ROCKSDB RANDOM READ THROUGHPUT MULTITHREADED (Set int -> boolean)", measures);
  }

  // UTILITY

  private Measure measure(int keysCount, int valueLength, double cacheSizeRatio, final boolean frequentReads,
                          boolean randomReads, int noOfThreads) throws RocksDBException {

    // Generate keys
    long seed = 4242;
    final Integer[] keys = GenerateTestData.generateRandomIntKeys(keysCount, Integer.MAX_VALUE, seed);

    // Write store
    File file = new File(TEST_FOLDER, "rocksdb" + keysCount + "-" + valueLength + ".store");
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setAllowMmapWrites(false);
    options.setAllowMmapReads(false);
    options.setCompressionType(CompressionType.NO_COMPRESSION);
    options.setCompactionStyle(CompactionStyle.LEVEL);
    options.setMaxOpenFiles(-1);

    var wOptions = new WriteOptions();
    wOptions.setDisableWAL(true);

    var fOptions = new FlushOptions();
    fOptions.setWaitForFlush(true);

    final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
    //tableOptions.setFilterPolicy(new BloomFilter(10, false));
    options.setTableFormatConfig(tableOptions);

    RocksDB db = null;
    try {
      db = RocksDB.open(options, file.getAbsolutePath());

      for (Integer key : keys) {
        if (valueLength == 0) {
          db.put(wOptions, key.toString().getBytes(), "1".getBytes());
        } else {
          db.put(wOptions, key.toString().getBytes(), RandomStringUtils.randomAlphabetic(valueLength).getBytes());
        }
      }
      db.flush(fOptions);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      wOptions.close();
      fOptions.close();
      if (db != null) {
        db.close();
      }
    }
    options.close();

    final ReadOptions readOptions = new ReadOptions();
    readOptions.setVerifyChecksums(false);

    // Open
    //NanoBench nanoBench = NanoBench.create();

      options = new Options();
      options.setAllowMmapWrites(false);
      options.setAllowMmapReads(false);
      options.setCompressionType(CompressionType.NO_COMPRESSION);
      options.setCompactionStyle(CompactionStyle.LEVEL);
      options.setMaxOpenFiles(-1);
      options.setTableFormatConfig(tableOptions);

      db = RocksDB.open(options, file.getAbsolutePath());
      final RocksDB reader = db;

      // Measure

      var totalCount = new AtomicInteger(0);
      var findCount = new AtomicInteger(0);

      // Measure
      NanoBench nanoBench = NanoBench.create();
      nanoBench.cpuOnly().warmUps(5).measurements(20).measure("Measure %d reads for %d keys with cache", () -> {
        if (noOfThreads < 2) {
          try {
            doWork(randomReads, keys, totalCount, findCount, reader, readOptions);
          } catch (RocksDBException e) {
            throw new RuntimeException(e);
          }
        } else {
          var forkJoinPool = new ForkJoinPool(noOfThreads);
          try {
            forkJoinPool.submit(() -> IntStream.range(0, noOfThreads).parallel()
                    .forEach(i -> {
                      try {
                        doWork(randomReads, keys, totalCount, findCount, reader, readOptions);
                      } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                      }
                    })
            ).join();
          } finally {
            forkJoinPool.shutdown();
          }
        }
      });

      // Return measure
      double rps = READS * noOfThreads * nanoBench.getTps();

/*      nanoBench.cpuOnly().warmUps(5).measurements(20)
          .measure("Measure %d reads for %d keys with cache", () -> {
            Random r = new Random();
            int length = keys.length;
            for (int i = 0; i < READS; i++) {
              int index;
              if (i % 2 == 0 && frequentReads) {
                index = r.nextInt(length / 10);
              } else {
                index = r.nextInt(length);
              }
              Integer key = keys[index];
              try {
                reader.get(readOptions, key.toString().getBytes());
              } catch (RocksDBException e) {

              }
            }
          });
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      if (db != null) {
        db.close();
      }
    }

    // Return measure
    double rps = READS * nanoBench.getTps();*/
    return new Measure(DirectoryUtils.folderSize(TEST_FOLDER), rps, valueLength, 0L, keys.length);
  }

  private void doWork(boolean randomReads, Integer[] keys, AtomicInteger totalCount, AtomicInteger findCount,
                      RocksDB reader, ReadOptions readOptions) throws RocksDBException {
    Random r = new Random(42);
    int length = keys.length;
    for (int j = 0; j < READS; j++) {
      totalCount.incrementAndGet();
      Integer key;
      if (randomReads) {
        key = r.nextInt(Integer.MAX_VALUE);
      } else {
        key = keys[r.nextInt(length)];
      }
      var value = reader.get(readOptions, key.toString().getBytes());
      if (value != null) {
        findCount.incrementAndGet();
      }
    }
  }

  private void report(String title, List<Measure> measures) {
    System.out.println(title + "\n\n");
    System.out.println("FILE LENGTH;KEYS;RPS");
    for (Measure m : measures) {
      System.out.println(m.fileSize + ";" + m.keys + ";" + m.rps);
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
