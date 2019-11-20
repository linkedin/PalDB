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

package com.linkedin.paldb.utils;

import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;


public class TestTempUtils {

  public static boolean deleteDirectory(File directoryToBeDeleted) {
    if (directoryToBeDeleted.isDirectory()) {
      File[] allContents = directoryToBeDeleted.listFiles();
      if (allContents != null) {
        for (File file : allContents) {
          deleteDirectory(file);
        }
      }
    }
    return directoryToBeDeleted.delete();
  }

  @Test
  public void testTempDir() {
    File file = TempUtils.createTempDir("foo");
    assertTrue(file.exists());
    assertTrue(file.isDirectory());
    assertTrue(file.getName().contains("foo"));
    file.delete();
  }

  @Test
  public void testCopyIntoTempFile()
      throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream("foo".getBytes());
    File file = TempUtils.copyIntoTempFile("bar", bis);
    assertTrue(file.exists());
    assertTrue(file.isFile());
    assertTrue(file.getName().contains("bar"));
    assertEquals(bis.available(), 0);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    FileInputStream fis = new FileInputStream(file);
    byte[] buffer = new byte[8192];
    int length;
    while ((length = fis.read(buffer)) > 0) {
      bos.write(buffer, 0, length);
    }
    fis.close();
    bos.close();
    assertArrayEquals(bos.toByteArray(), "foo".getBytes());
  }
}

