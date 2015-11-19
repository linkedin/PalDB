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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * Temporary files utility class.
 */
public final class TempUtils {

  // Default constructor
  private TempUtils() {

  }

  /**
   * Creates a temporary directory prefixed with <code>prefix</code>.
   *
   * @param prefix folder prefix
   * @return temporary folder
   */
  public static File createTempDir(String prefix) {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    String baseName = prefix + System.currentTimeMillis() + "-";

    for (int counter = 0; counter < 10000; counter++) {
      File tempDir = new File(baseDir, baseName + counter);
      if (tempDir.mkdir()) {
        return tempDir;
      }
    }
    throw new IllegalStateException(
        "Failed to create directory within " + 10000 + " attempts (tried " + baseName + "0 to " + baseName + (10000 - 1)
            + ')');
  }
  
  /**
   * Copies <code>inputStream</code> into a temporary file <code>fileName</code>.
   *
   * @param fileName file name
   * @param inputStream stream to copy from
   * @return temporary file
   * @throws IOException if an IO error occurs
   */
  public static File copyIntoTempFile(String fileName, InputStream inputStream)
      throws IOException {
    BufferedInputStream bufferedStream = inputStream instanceof BufferedInputStream ? (BufferedInputStream) inputStream
        : new BufferedInputStream(inputStream);
    File destFile = null;
    try {
      destFile = File.createTempFile(fileName, null);
      destFile.deleteOnExit();

      FileOutputStream fileOutputStream = new FileOutputStream(destFile);
      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
      try {
        byte[] buffer = new byte[8192];
        int length;
        while ((length = bufferedStream.read(buffer)) > 0) {
          bufferedOutputStream.write(buffer, 0, length);
        }
      } finally {
        bufferedOutputStream.close();
        fileOutputStream.close();
      }
    } finally {
      bufferedStream.close();
      inputStream.close();
    }
    return destFile;
  }
}
