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

import com.linkedin.paldb.api.Configuration;
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.StoreReader;
import com.linkedin.paldb.api.StoreWriter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;


public class TestStoreReaderMultiThreads {

  private final File STORE_FOLDER = new File("data");
  private final File STORE_FILE = new File(STORE_FOLDER, "paldb.dat");
  private final File STORE_FILE2 = new File(STORE_FOLDER, "paldb2.dat");
  private StoreReader reader;
  private StoreReader reader2;

  private final Object[] testValues =
      new Object[]{1.0f,"foo",1};

  private final Object[] testValues2 =
      new Object[]{1.0f,"foo",1,2,3,1};

  @BeforeMethod
  public void setUp() {
    STORE_FILE.delete();
    STORE_FILE2.delete();
    STORE_FOLDER.delete();
    STORE_FOLDER.mkdir();

    Configuration config = new Configuration();
    writePaldb(STORE_FILE, testValues, config);
    writePaldb(STORE_FILE2, testValues2, config);

    reader = PalDB.createReader(STORE_FILE, config);
    reader2 = PalDB.createReader(STORE_FILE2, config);
  }

  private void writePaldb(File storeFile, Object[] testValues, Configuration config) {
    StoreWriter writer = PalDB.createWriter(storeFile, config);
    for (int i = 0; i < testValues.length; i++) {
      writer.put(i, testValues[i]);
    }
    writer.close();
  }

  @AfterMethod
  public void cleanUp() {
    try {
      reader.close();
      reader2.close();
    } catch (Exception e) {
    }
    STORE_FILE.delete();
    STORE_FILE2.delete();
    STORE_FOLDER.delete();
  }


  @Test
  public void testMultiThreadRead()
      throws Throwable {
    int threadCount = 2;
    final CountDownLatch latch = new CountDownLatch(threadCount);
    final AtomicBoolean success = new AtomicBoolean(true);
    for(int i=0;i<threadCount;i++){
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            for(int c=0;c<100000;c++){
              if(!success.get())break;
              Assert.assertEquals(reader.getString(1), "foo");
              Assert.assertEquals(reader.getFloat(0, 0f), 1f, 0.000001);
            }
          } catch (Throwable error){
            error.printStackTrace();
              success.set(false);
          } finally {
              latch.countDown();
          }
        }
      }).start();
    }
    latch.await();
    Assert.assertTrue(success.get());

  }

  @Test
  public void testThreadPoolRead()
          throws Throwable {
    int threadCount = 2;
    final CountDownLatch latch = new CountDownLatch(threadCount);
    final AtomicBoolean success = new AtomicBoolean(true);
    final ExecutorService poolExecutor = Executors.newSingleThreadExecutor();
    poolExecutor.submit(new ReadTask(success, latch, reader, 2));
    poolExecutor.submit(new ReadTask(success, latch, reader2, 5));
    latch.await();
    Assert.assertTrue(success.get());

  }

  private class ReadTask implements Runnable {
    private final AtomicBoolean success;
    private final CountDownLatch latch;
    private StoreReader reader;
    private int index;

    public ReadTask(AtomicBoolean success, CountDownLatch latch, StoreReader reader, int index) {
      this.success = success;
      this.latch = latch;
      this.reader = reader;
      this.index = index;
    }

    @Override
    public void run() {
      try {
        for(int c=0;c<100000;c++){
          if(!success.get())break;
          Assert.assertEquals(reader.getString(1), "foo");
          Assert.assertEquals(reader.getInt(index, 0), 1);
        }
      } catch (Throwable error){
        error.printStackTrace();
          success.set(false);
      } finally {
          latch.countDown();
      }
    }
  }
}
