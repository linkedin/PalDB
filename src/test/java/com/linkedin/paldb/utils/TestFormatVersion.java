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

import static org.junit.jupiter.api.Assertions.*;

class TestFormatVersion {

  @Test
  void testIs() {
    assertTrue(FormatVersion.PALDB_V1.is(FormatVersion.PALDB_V1));
  }

  @Test
  void testBytes() {
    assertEquals(FormatVersion.PALDB_V1, FormatVersion.fromBytes(FormatVersion.PALDB_V1.getBytes()));
  }

  @Test
  void testPrefixBytes() {
    assertArrayEquals("PALDB".getBytes(), FormatVersion.getPrefixBytes());
  }

  @Test
  void testGetLastVersion() {
    assertEquals(FormatVersion.getLatestVersion(), FormatVersion.values()[FormatVersion.values().length - 1]);
  }
}
