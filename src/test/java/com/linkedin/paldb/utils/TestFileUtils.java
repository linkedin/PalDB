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

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TestFileUtils {

  @Test
  void testTempDir() {
    File file = FileUtils.createTempDir("foo");
    assertTrue(file.exists());
    assertTrue(file.isDirectory());
    assertTrue(file.getName().contains("foo"));
    assertTrue(file.delete());
  }

  @Test
  void testCopyIntoTempFile() throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream("foo".getBytes());
    File file = FileUtils.copyIntoTempFile("bar", bis);
    assertTrue(file.exists());
    assertTrue(file.isFile());
    assertTrue(file.getName().contains("bar"));
    assertEquals(0, bis.available());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    FileInputStream fis = new FileInputStream(file);
    byte[] buffer = new byte[8192];
    int length;
    while ((length = fis.read(buffer)) > 0) {
      bos.write(buffer, 0, length);
    }
    fis.close();
    bos.close();
    assertArrayEquals("foo".getBytes(), bos.toByteArray());
  }

  @Test
  void should_delete_files_and_dir() throws IOException {
    var testDir = Files.createTempDirectory("testDir");

    for (int i = 0; i < 10; i++) {
      Files.createTempFile(testDir, "tmp1", ".tmp");
    }

    assertEquals(10, Files.list(testDir).count());

    FileUtils.deleteDirectory(testDir.toFile());
    assertFalse(Files.exists(testDir ));
  }

  @Test
  void should_create_temp_file() throws IOException {
    var tempFile = FileUtils.createTempFile("test", ".paldb");
    assertTrue(tempFile.exists());
    Files.delete(tempFile.toPath());
  }

  @Test
  void should_throw_when_trying_to_delete_used_file(@TempDir Path tempDir) throws IOException {
    var file = tempDir.resolve("test.dat").toFile();
    assertTrue(file.createNewFile());

    try (var fileOutputStream = new FileOutputStream(file)) {
      fileOutputStream.write(10);
      assertFalse(FileUtils.deleteDirectory(file));
    }
  }
}

