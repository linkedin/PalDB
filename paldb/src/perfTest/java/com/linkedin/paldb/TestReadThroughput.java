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

import com.linkedin.paldb.api.Configuration;
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.StoreReader;
import com.linkedin.paldb.api.StoreWriter;
import com.linkedin.paldb.impl.GenerateTestData;
import com.linkedin.paldb.utils.DirectoryUtils;
import com.linkedin.paldb.utils.NanoBench;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestReadThroughput {

  private File TEST_FOLDER = new File("testreadthroughput");
  private final int READS = 500000;

  @BeforeMethod
  public void setUp() {
    DirectoryUtils.deleteDirectory(TEST_FOLDER);
    TEST_FOLDER.mkdir();
  }

  @AfterMethod
  public void cleanUp() {
    DirectoryUtils.deleteDirectory(TEST_FOLDER);
  }

  @Test
  public void testReadThroughput() {

    List<Measure> measures = new ArrayList<Measure>();
    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {
      Measure m = measure(i, 0, 0, false);

      measures.add(m);
    }

    report("READ THROUGHPUT (Set int -> boolean)", measures);
  }

  @Test
  public void testReadThroughputWithCache() {

    List<Measure> measures = new ArrayList<Measure>();
    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {
      Measure m = measure(i, 0, 0.2, false);

      measures.add(m);
    }

    report("READ THROUGHPUT WITH CACHE (Set int -> boolean)", measures);
  }

  // UTILITY

  private Measure measure(int keysCount, int valueLength, double cacheSizeRatio, final boolean frequentReads) {

    // Generate keys
    long seed = 4242;
    final Integer[] keys = GenerateTestData.generateRandomIntKeys(keysCount, Integer.MAX_VALUE, seed);

    // Write store
    File storeFile = new File(TEST_FOLDER, "paldb" + keysCount + "-" + valueLength + ".store");
    StoreWriter writer = PalDB.createWriter(storeFile, new Configuration());
    for (Integer key : keys) {
      if (valueLength == 0) {
        writer.put(key.toString(), Boolean.TRUE);
      } else {
        writer.put(key.toString(), RandomStringUtils.randomAlphabetic(valueLength));
      }
    }
    writer.close();

    // Get reader
    long cacheSize = 0;
    Configuration config = PalDB.newConfiguration();
    if (cacheSizeRatio > 0) {
      cacheSize = (long) (storeFile.length() * cacheSizeRatio);
      config.set(Configuration.CACHE_ENABLED, "true");
      config.set(Configuration.CACHE_BYTES, String.valueOf(cacheSize));
    } else {
      config.set(Configuration.CACHE_ENABLED, "false");
    }
    final StoreReader reader = PalDB.createReader(storeFile, config);

    // Measure
    NanoBench nanoBench = NanoBench.create();
    nanoBench.cpuOnly().warmUps(5).measurements(20).measure("Measure %d reads for %d keys with cache", new Runnable() {
      @Override
      public void run() {
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
          reader.get(key.toString());
        }
      }
    });

    // Close
    reader.close();

    // Return measure
    double rps = READS * nanoBench.getTps();
    return new Measure(storeFile.length(), rps, valueLength, cacheSize, keys.length);
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
