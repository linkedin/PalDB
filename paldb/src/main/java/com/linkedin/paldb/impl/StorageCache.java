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
import com.linkedin.paldb.api.Serializer;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * LRU cache configured on the desired size in memory.
 * <dl>
 *   <dt>This cache can be configured with the following properties:</dt>
 *   <dd><code>cache.enabled</code> - LRU cache enabled</dd>
 *   <dd><code>cache.bytes</code> - cache limit (bytes)</dd>
 *   <dd><code>cache.initial.capacity</code> - cache initial capacity</dd>
 *   <dd><code>cache.load.factor</code> - cache load factor</dd>
 * </dl>
 * <p>
 * The cache estimates the size of the objects it contains so it consumes no more than the configured
 * memory limit.
 */
public class StorageCache {
  // Static null object to recognizes null from missing values
  protected static final Object NULL_VALUE = new Object();

  //Logger
  private final static Logger LOGGER = Logger.getLogger(StorageCache.class.getName());

  /**
   * Factory to create and initialize the cache.
   *
   * @param configuration configuration
   * @return new cache
   */
  static StorageCache initCache(Configuration configuration) {
    if (configuration.getBoolean(Configuration.CACHE_ENABLED) && configuration.getLong(Configuration.CACHE_BYTES) > 0) {
      return new StorageCache(configuration);
    } else {
      return new DisabledCache();
    }
  }

  /*
   * Memory usage
   *  632 bytes for the LinkedHashMap
   *  24 bytes theoretical overhead per entry but more like 45 in practice
   */
  final static int OVERHEAD = 50;
  private final LinkedHashMap cache;
  private final Configuration configuration;
  private long maxWeight;
  private long currentWeight;

  /**
   * Cache constructor.
   *
   * @param config configuration
   */
  private StorageCache(Configuration config) {
    cache = new LinkedHashMap(config.getInt(Configuration.CACHE_INITIAL_CAPACITY),
        config.getFloat(Configuration.CACHE_LOAD_FACTOR), true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry eldest) {
        boolean res = currentWeight > maxWeight;
        if (res) {
          Object key = eldest.getKey();
          Object value = eldest.getValue();
          currentWeight -= getWeight(key) + getWeight(value) + OVERHEAD;
        }
        return res;
      }
    };
    maxWeight = config.getLong(Configuration.CACHE_BYTES);
    LOGGER.log(Level.INFO, "Cache initialized with maximum {0} Mb usage",
        new DecimalFormat("#,##0.00").format(maxWeight / (1024.0 * 1024.0)));
    configuration = config;
  }

  /**
   * Private constructor used by the <code>DisabledCache</code> inner class.
   */
  private StorageCache() {
    cache = null;
    configuration = null;
  }

  /**
   * Gets the value in the cache for <code>key</code> or null if not found.
   * <p>
   * If the value associated with <code>key</code> exists but is null, returns
   * <code>StorageCache.NULL_VALUE</code>.
   *
   * @param key key to get value for
   * @param <K> return type
   * @return value, null or <code>StorageCache.NULL_VALUE</code>
   */
  public <K> K get(Object key) {
    return (K) cache.get(key);
  }

  /**
   * Returns true if the cache contains <code>key</code>.
   *
   * @param key key to test presence for
   * @return true if found, false otherwise
   */
  public boolean contains(Object key) {
    return cache.containsKey(key);
  }

  /**
   * Puts the <code>key/value</code> pair into the cache.
   *
   * @param key key
   * @param value value
   */
  public void put(Object key, Object value) {
    int weight = getWeight(key) + getWeight(value) + OVERHEAD;
    currentWeight += weight;
    if (cache.put(key, value == null ? NULL_VALUE : value) != null) {
      currentWeight -= weight;
    }
  }

  /**
   * Gets the weight for <code>value</code>.
   *
   * @param value value to get weight for
   * @return weight
   */
  private int getWeight(Object value) {
    if (value == null) {
      return 0;
    }
    if (value.getClass().isArray()) {
      Class cc = value.getClass().getComponentType();
      if (cc.isPrimitive()) {
        if (cc.equals(int.class)) {
          return ((int[]) value).length * 4;
        } else if (cc.equals(long.class)) {
          return ((long[]) value).length * 8;
        } else if (cc.equals(double.class)) {
          return ((double[]) value).length * 8;
        } else if (cc.equals(float.class)) {
          return ((float[]) value).length * 4;
        } else if (cc.equals(boolean.class)) {
          return ((boolean[]) value).length * 1;
        } else if (cc.equals(byte.class)) {
          return ((byte[]) value).length * 1;
        } else if (cc.equals(short.class)) {
          return ((short[]) value).length * 2;
        } else if (cc.equals(char.class)) {
          return ((char[]) value).length * 2;
        }
      } else if (cc.equals(String.class)) {
        String[] v = (String[]) value;
        int res = 0;
        for (int i = 0; i < v.length; i++) {
          res += v[i].length() * 2 + 40;
        }
        return res;
      } else if (cc.equals(int[].class)) {
        int[][] v = (int[][]) value;
        int res = 0;
        for (int i = 0; i < v.length; i++) {
          res += v[i].length * 4;
        }
        return res;
      } else if (cc.equals(long[].class)) {
        long[][] v = (long[][]) value;
        int res = 0;
        for (int i = 0; i < v.length; i++) {
          res += v[i].length * 8;
        }
        return res;
      } else {
        Object[] v = (Object[]) value;
        int res = 0;
        for (int i = 0; i < v.length; i++) {
          res += getWeight(v[i]);
        }
        return res;
      }
    } else if (value instanceof String) {
      return ((String) value).length() * 2 + 40;
    } else {
      Serializer serializer = configuration.getSerializer(value.getClass());
      if (serializer != null) {
        return serializer.getWeight(value);
      }
    }
    return 16;
  }

  /**
   * Sets the max weight in the cache.
   *
   * @param maxWeight max weight
   */
  public void setMaxWeight(long maxWeight) {
    this.maxWeight = maxWeight;
  }

  /**
   * Gets the cache size.
   *
   * @return cache size
   */
  public int size() {
    return cache.size();
  }

  /**
   * Gets the cache current weight.
   *
   * @return weight
   */
  public long getWeight() {
    return currentWeight;
  }

  /**
   * Special inner class that overrides all cache's features when the cache is disabled.
   */
  private static class DisabledCache extends StorageCache {

    DisabledCache() {
      LOGGER.log(Level.INFO, "Cache disabled");
    }

    @Override
    public Object get(Object key) {
      return null;
    }

    @Override
    public boolean contains(Object key) {
      return false;
    }

    @Override
    public void put(Object key, Object value) {
    }

    @Override
    public int size() {
      return 0;
    }
  }
}
