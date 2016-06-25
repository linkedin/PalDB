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
import com.linkedin.paldb.api.Serializer;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestStorageCache {

  private final static int ENTRY_SIZE = 16 * 2 + StorageCache.OVERHEAD;

  private Configuration _configuration;

  @BeforeMethod
  public void setUp() {
    _configuration = PalDB.newConfiguration();
    _configuration.set(Configuration.CACHE_ENABLED, "true");
  }

  @Test
  public void testContainsValid() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, 0);
    Assert.assertTrue(cache.contains(0));
  }

  @Test
  public void testContainsInValid() {
    StorageCache cache = StorageCache.initCache(_configuration);
    Assert.assertFalse(cache.contains(0));
  }

  @Test
  public void testEmpty() {
    StorageCache cache = StorageCache.initCache(_configuration);
    Assert.assertNull(cache.get(0));
    Assert.assertEquals(cache.size(), 0);
  }

  @Test
  public void testPutOneItem() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, 0);
    Assert.assertNotNull(cache.get(0));
    Assert.assertEquals(cache.size(), 1);
  }

  @Test
  public void testPutTwice() {
    Integer second = 1;
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, 1);
    cache.put(0, second);
    Assert.assertSame(cache.get(0), second);
  }

  @Test
  public void testPutZeroSize() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.setMaxWeight(0);
    cache.put(0, 1);
    Assert.assertEquals(cache.size(), 0);
  }

  @Test
  public void testPutTwiceObjectSize() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.setMaxWeight(ENTRY_SIZE);
    cache.put(0, 0);
    cache.put(1, 1);
    Assert.assertEquals(cache.size(), 1);
    Assert.assertNull(cache.get(0));
    Assert.assertNotNull(cache.get(1));
  }

  @Test
  public void putSameCheckWeight() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, 0);
    long weight = cache.getWeight();
    cache.put(0, 0);
    Assert.assertEquals(cache.getWeight(), weight);
  }

  @Test
  public void testPutGet() {
    int objs = 100;
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.setMaxWeight(ENTRY_SIZE * objs);
    for (int i = 0; i < objs; i++) {
      cache.put(i, i);
    }
    Assert.assertEquals(cache.size(), 100);
    for (int i = 0; i < objs; i++) {
      Assert.assertNotNull(cache.get(i));
    }
  }

  @Test
  public void testCheckOrder() {
    int objs = 100;
    int capacity = 50;
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.setMaxWeight(ENTRY_SIZE * capacity);
    for (int i = 0; i < objs; i++) {
      cache.put(i, i);
    }
    Assert.assertEquals(cache.size(), capacity);
    for (int i = 0; i < objs; i++) {
      if (i < capacity) {
        Assert.assertNull(cache.get(i));
      } else {
        Assert.assertNotNull(cache.get(i));
      }
    }
  }

  @Test
  public void testCheckAccessOrderGet() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.setMaxWeight(ENTRY_SIZE * 3);
    cache.put(0, 0);
    cache.put(1, 1);
    cache.get(0);
    cache.put(2, 2);
    Assert.assertEquals(cache.size(), 3);
    cache.put(3, 2);
    Assert.assertNull(cache.get(1));
    Assert.assertNotNull(cache.get(0));
  }

  @Test
  public void testCheckAccessOrderPut() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.setMaxWeight(ENTRY_SIZE * 3);
    cache.put(0, 0);
    cache.put(1, 1);
    cache.put(0, 0);
    cache.put(2, 2);
    Assert.assertEquals(cache.size(), 3);
    cache.put(3, 2);
    Assert.assertNull(cache.get(1));
    Assert.assertNotNull(cache.get(0));
  }

  @Test
  public void testWeightKeyObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, 0);
    Assert.assertEquals(cache.getWeight(), ENTRY_SIZE);
  }

  @Test
  public void testWeightKeyArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(new Object[]{0, 1}, 0);
    Assert.assertEquals(cache.getWeight(), 16 + 32 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueIntArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new int[]{1, 2});
    Assert.assertEquals(cache.getWeight(), 16 + 8 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueLongArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new long[]{1, 2});
    Assert.assertEquals(cache.getWeight(), 16 + 16 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueDoubleArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new double[]{1.0, 2.0});
    Assert.assertEquals(cache.getWeight(), 16 + 16 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueFloatArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new float[]{1.0F, 2.0F});
    Assert.assertEquals(cache.getWeight(), 16 + 8 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueBooleanArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new boolean[]{true, false});
    Assert.assertEquals(cache.getWeight(), 16 + 2 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueByteArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new byte[]{1, 2});
    Assert.assertEquals(cache.getWeight(), 16 + 2 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueShortArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new short[]{1, 2});
    Assert.assertEquals(cache.getWeight(), 16 + 4 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueCharArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new char[]{'a', 'b'});
    Assert.assertEquals(cache.getWeight(), 16 + 4 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueStringArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new String[]{"one", "two"});
    Assert.assertEquals(cache.getWeight(), 16 + 46 * 2 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueInt2DArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new int[][]{{1, 2}, {3, 4}});
    Assert.assertEquals(cache.getWeight(), 16 + 8 * 2 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueLong2DArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new long[][]{{1, 2}, {3, 4}});
    Assert.assertEquals(cache.getWeight(), 16 + 16 * 2 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueStringObject() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new String("one"));
    Assert.assertEquals(cache.getWeight(), 16 + 46 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueObjectArrayObjects() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, new Object[]{0, 1});
    Assert.assertEquals(cache.getWeight(), 16 + 32 + StorageCache.OVERHEAD);
  }

  @Test
  public void testNullValue() {
    StorageCache cache = StorageCache.initCache(_configuration);
    cache.put(0, null);
    Assert.assertEquals(cache.size(), 1);
    Assert.assertEquals(cache.get(0), StorageCache.NULL_VALUE);
  }

  @Test
  public void testDisabled() {
    Configuration configuration = new Configuration();
    configuration.set(Configuration.CACHE_ENABLED, "false");
    StorageCache cache = StorageCache.initCache(configuration);
    Assert.assertEquals(cache.size(), 0);
    Assert.assertNull(cache.get("foo"));
    Assert.assertFalse(cache.contains("foo"));
  }

  @Test
  public void testDisabledPut() {
    Configuration configuration = new Configuration();
    configuration.set(Configuration.CACHE_ENABLED, "false");
    StorageCache cache = StorageCache.initCache(configuration);
    cache.put(0, "foo");
    Assert.assertEquals(cache.size(), 0);
    Assert.assertNull(cache.get("foo"));
    Assert.assertFalse(cache.contains("foo"));
  }
}
