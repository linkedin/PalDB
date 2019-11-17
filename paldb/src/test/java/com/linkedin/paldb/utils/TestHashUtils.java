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

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHashUtils {

  @Test
  public void testHashEquals() {
    Assert.assertEquals(HashUtils.hash("foo".getBytes()), HashUtils.hash("foo".getBytes()));
  }

  @Test
  public void testEmpty() {
    Assert.assertTrue(HashUtils.hash(new byte[0]) > 0);
  }
}
