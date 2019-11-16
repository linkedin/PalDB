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

import com.linkedin.paldb.api.*;
import org.testng.Assert;
import org.testng.annotations.*;


public class TestStorageCache {

  private static final int ENTRY_SIZE = 16 * 2 + StorageCache.OVERHEAD;

  private Configuration configuration;

  @BeforeMethod
  public void setUp() {
    configuration = PalDB.newConfiguration();
    configuration.set(Configuration.CACHE_ENABLED, "true");
  }

  @Test
  public void testContainsValid() {
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
    cache.put(0, 0);
    Assert.assertTrue(cache.contains(0));
  }

  @Test
  public void testContainsInValid() {
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
    Assert.assertFalse(cache.contains(0));
  }

  @Test
  public void testEmpty() {
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
    Assert.assertNull(cache.get(0));
    Assert.assertEquals(cache.size(), 0);
  }

  @Test
  public void testPutOneItem() {
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
    cache.put(0, 0);
    Assert.assertNotNull(cache.get(0));
    Assert.assertEquals(cache.size(), 1);
  }

  @Test
  public void testPutTwice() {
    Integer second = 1;
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
    cache.put(0, 1);
    cache.put(0, second);
    Assert.assertSame(cache.get(0), second);
  }

  @Test
  public void testPutZeroSize() {
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
    cache.setMaxWeight(0);
    cache.put(0, 1);
    Assert.assertEquals(cache.size(), 0);
  }

  @Test
  public void testPutTwiceObjectSize() {
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
    cache.setMaxWeight(ENTRY_SIZE);
    cache.put(0, 0);
    cache.put(1, 1);
    Assert.assertEquals(cache.size(), 1);
    Assert.assertNull(cache.get(0));
    Assert.assertNotNull(cache.get(1));
  }

  @Test
  public void putSameCheckWeight() {
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
    cache.put(0, 0);
    long weight = cache.getWeight();
    cache.put(0, 0);
    Assert.assertEquals(cache.getWeight(), weight);
  }

  @Test
  public void testPutGet() {
    int objs = 100;
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
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
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
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
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
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
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
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
    StorageCache<Integer, Integer> cache = StorageCache.initCache(configuration);
    cache.put(0, 0);
    Assert.assertEquals(cache.getWeight(), ENTRY_SIZE);
  }

  @Test
  public void testWeightKeyArrayObjects() {
    StorageCache<Object[], Integer> cache = StorageCache.initCache(configuration);
    cache.put(new Object[]{0, 1}, 0);
    Assert.assertEquals(cache.getWeight(), 16 + 32 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueIntArrayObjects() {
    StorageCache<Integer,int[]> cache = StorageCache.initCache(configuration);
    cache.put(0, new int[]{1, 2});
    Assert.assertEquals(cache.getWeight(), 16 + 8 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueLongArrayObjects() {
    StorageCache<Integer, long[]> cache = StorageCache.initCache(configuration);
    cache.put(0, new long[]{1, 2});
    Assert.assertEquals(cache.getWeight(), 16 + 16 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueDoubleArrayObjects() {
    StorageCache<Integer,double[]> cache = StorageCache.initCache(configuration);
    cache.put(0, new double[]{1.0, 2.0});
    Assert.assertEquals(cache.getWeight(), 16 + 16 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueFloatArrayObjects() {
    StorageCache<Integer,float[]> cache = StorageCache.initCache(configuration);
    cache.put(0, new float[]{1.0F, 2.0F});
    Assert.assertEquals(cache.getWeight(), 16 + 8 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueBooleanArrayObjects() {
    StorageCache<Integer,boolean[]> cache = StorageCache.initCache(configuration);
    cache.put(0, new boolean[]{true, false});
    Assert.assertEquals(cache.getWeight(), 16 + 2 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueByteArrayObjects() {
    StorageCache<Integer,byte[]> cache = StorageCache.initCache(configuration);
    cache.put(0, new byte[]{1, 2});
    Assert.assertEquals(cache.getWeight(), 16 + 2 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueShortArrayObjects() {
    StorageCache<Integer,short[]> cache = StorageCache.initCache(configuration);
    cache.put(0, new short[]{1, 2});
    Assert.assertEquals(cache.getWeight(), 16 + 4 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueCharArrayObjects() {
    StorageCache<Integer,char[]> cache = StorageCache.initCache(configuration);
    cache.put(0, new char[]{'a', 'b'});
    Assert.assertEquals(cache.getWeight(), 16 + 4 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueStringArrayObjects() {
    StorageCache<Integer,String[]> cache = StorageCache.initCache(configuration);
    cache.put(0, new String[]{"one", "two"});
    Assert.assertEquals(cache.getWeight(), 16 + 46 * 2 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueInt2DArrayObjects() {
    StorageCache<Integer,int[][]> cache = StorageCache.initCache(configuration);
    cache.put(0, new int[][]{{1, 2}, {3, 4}});
    Assert.assertEquals(cache.getWeight(), 16 + 8 * 2 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueLong2DArrayObjects() {
    StorageCache<Integer,long[][]> cache = StorageCache.initCache(configuration);
    cache.put(0, new long[][]{{1, 2}, {3, 4}});
    Assert.assertEquals(cache.getWeight(), 16 + 16 * 2 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueStringObject() {
    StorageCache<Integer,String> cache = StorageCache.initCache(configuration);
    cache.put(0, new String("one"));
    Assert.assertEquals(cache.getWeight(), 16 + 46 + StorageCache.OVERHEAD);
  }

  @Test
  public void testWeightValueObjectArrayObjects() {
    StorageCache<Integer,Object[]> cache = StorageCache.initCache(configuration);
    cache.put(0, new Object[]{0, 1});
    Assert.assertEquals(cache.getWeight(), 16 + 32 + StorageCache.OVERHEAD);
  }

  @Test
  public void testNullValue() {
    StorageCache<Integer,byte[]> cache = StorageCache.initCache(configuration);
    cache.put(0, null);
    Assert.assertEquals(cache.size(), 1);
    Assert.assertEquals(cache.get(0), StorageCache.NULL_VALUE);
  }

  @Test
  public void testDisabled() {
    Configuration cfg = new Configuration();
    cfg.set(Configuration.CACHE_ENABLED, "false");
    StorageCache<String,String> cache = StorageCache.initCache(cfg);
    Assert.assertEquals(cache.size(), 0);
    Assert.assertNull(cache.get("foo"));
    Assert.assertFalse(cache.contains("foo"));
  }

  @Test
  public void testDisabledPut() {
    Configuration cfg = new Configuration();
    cfg.set(Configuration.CACHE_ENABLED, "false");
    StorageCache<Integer,String> cache = StorageCache.initCache(cfg);
    cache.put(0, "foo");
    Assert.assertEquals(cache.size(), 0);
    Assert.assertNull(cache.get(1));
    Assert.assertFalse(cache.contains(2));
  }
}
