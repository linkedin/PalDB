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

import com.linkedin.paldb.impl.GenerateTestData;
import com.linkedin.paldb.utils.DirectoryUtils;
import com.linkedin.paldb.utils.NanoBench;
import net.openhft.chronicle.map.ChronicleMap;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class TestReadThroughputChronicleMap {

  private File TEST_FOLDER = new File("testreadthroughputchroniclemap");
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

  // UTILITY

  private Measure measure(int keysCount, int valueLength, double cacheSizeRatio, final boolean frequentReads) {

    // Generate keys
    long seed = 4242;
    final Integer[] keys = GenerateTestData.generateRandomIntKeys(keysCount, Integer.MAX_VALUE, seed);

    // Write store
    Class valueClass = valueLength > 0 ? String.class : Boolean.class;
    Object averageValue = valueLength > 0 ? RandomStringUtils.randomAlphabetic(valueLength) : true;
    File storeFile = new File(TEST_FOLDER, "chroniclemap" + keysCount + "-" + valueLength + ".store");
    //noinspection unchecked
    try (ChronicleMap<String, Object> map = ChronicleMap
            .of(String.class, valueClass)
            .averageKey(Integer.MAX_VALUE + "")
            .constantValueSizeBySample(averageValue)
            .entries(keysCount)
            .actualSegments(1)
            .checksumEntries(false)
            .createPersistedTo(storeFile)) {
      for (Integer key : keys) {
        map.put(key.toString(), valueLength > 0 ? RandomStringUtils.randomAlphabetic(valueLength) : true);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    NanoBench nanoBench = NanoBench.create();
    // Get reader
    //noinspection unchecked
    try (ChronicleMap<String, Object> reader =
                 ChronicleMap.of(String.class, valueClass).createPersistedTo(storeFile)) {

      // Measure
      nanoBench.cpuOnly().warmUps(5).measurements(20)
              .measure("Measure %d reads for %d keys with cache", new Runnable() {
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

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Return measure
    double rps = READS * nanoBench.getTps();
    return new Measure(storeFile.length(), rps, valueLength, 0L, keys.length);
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
