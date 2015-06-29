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
import com.linkedin.paldb.api.StoreWriter;
import com.linkedin.paldb.impl.GenerateTestData;
import com.linkedin.paldb.utils.DirectoryUtils;
import java.io.File;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestStoreSize {

  private File TEST_FOLDER = new File("teststoresize");

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
  public void testStoreSizeSet() {

    System.out.println("STORE SIZE (SET int -> boolean)\n\n");
    System.out.println("FILE LENGTH;KEYS;BYTES_PER_KEY");

    int max = 10000000;
    for (int i = 100; i <= max; i *= 10) {

      // Prepare store
      final Integer[] keys = GenerateTestData.generateRandomIntKeys(i, Integer.MAX_VALUE);
      final File storeFile = new File(TEST_FOLDER, i + ".store");
      StoreWriter writer = PalDB.createWriter(storeFile, new Configuration());
      for (Integer key : keys) {
        writer.put(key, Boolean.TRUE);
      }
      writer.close();

      long fileSize = storeFile.length();
      double sizePerKey = fileSize / (double) i;

      System.out.println(fileSize + ";" + i + ";" + sizePerKey);
    }
  }
}
