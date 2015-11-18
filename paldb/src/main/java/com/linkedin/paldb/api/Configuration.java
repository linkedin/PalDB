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

package com.linkedin.paldb.api;

import com.linkedin.paldb.impl.Serializers;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Store configuration.
 * <dl>
 *   <dt>This class recognizes the following property keys:</dt>
 *   <dd><code>mmap.segment.size</code> - memory map segment size (bytes) [default: 1GB]</dd>
 *   <dd><code>mmap.data.enabled</code> - enable memory mapping for data (boolean) [default: true]</dd>
 *   <dd><code>load.factor</code> - index load factor (double) [default: 0.75]</dd>
 *   <dd><code>cache.enabled</code> - LRU cache enabled (boolean) [default: false]</dd>
 *   <dd><code>cache.bytes</code> - cache limit (bytes) [default: Xmx - 100MB]</dd>
 *   <dd><code>cache.initial.capacity</code> - cache initial capacity (int) [default: 1000]</dd>
 *   <dd><code>cache.load.factor</code> - cache load factor (double) [default: 0.75]</dd>
 *   <dd><code>compression.enabled</code> - enable compression (boolean) [default: false]</dd>
 * </dl>
 * <p>
 * Default values can be set by setting properties to the JVM (ex:
 * -Dpaldb.mmap.data.enabled=false). All property names should be prefixed
 * with <em>paldb</em>.
 */
public class Configuration implements Serializable {

  // Buffer segment size
  public final static String MMAP_SEGMENT_SIZE = "mmap.segment.size";
  // Enable memory mapping for data
  public final static String MMAP_DATA_ENABLED = "mmap.data.enabled";
  // Load factor
  public final static String LOAD_FACTOR = "load.factor";
  // Cache enabled
  public final static String CACHE_ENABLED = "cache.enabled";
  // Cache limit (in bytes)
  public final static String CACHE_BYTES = "cache.bytes";
  // Cache initial capacity
  public final static String CACHE_INITIAL_CAPACITY = "cache.initial.capacity";
  // Cache load factor
  public final static String CACHE_LOAD_FACTOR = "cache.load.factor";
  // Enable compression
  public final static String COMPRESSION_ENABLED = "compression.enabled";

  // Property map
  protected final Map<String, String> properties = new HashMap<String, String>();
  // Read only
  protected final boolean readOnly;
  // Serializers
  protected final Serializers serializers;

  /**
   * Default constructor that initializes default values.
   */
  public Configuration() {
    readOnly = false;

    //Default
    putWithSystemPropertyDefault(MMAP_SEGMENT_SIZE, "1073741824");
    putWithSystemPropertyDefault(MMAP_DATA_ENABLED, "true");
    putWithSystemPropertyDefault(LOAD_FACTOR, "0.75");
    putWithSystemPropertyDefault(CACHE_ENABLED, "false");
    putWithSystemPropertyDefault(CACHE_INITIAL_CAPACITY, "1000");
    putWithSystemPropertyDefault(CACHE_LOAD_FACTOR, "0.75");
    putWithSystemPropertyDefault(COMPRESSION_ENABLED, "false");

    //Default cache size: (Xmx - 100mo);
    long cacheMemory = Math.max(0, Runtime.getRuntime().maxMemory() - (100 * 1024 * 1024));
    putWithSystemPropertyDefault(CACHE_BYTES, String.valueOf(cacheMemory));

    //Serializers
    serializers = new Serializers();
  }

  /**
   * Private constructor that initializes a read-only copy of <code>configuration</code>.
   *
   * @param configuration configuration to copy values from
   */
  Configuration(Configuration configuration) {
    readOnly = true;
    properties.putAll(configuration.properties);
    serializers = configuration.serializers;
  }

  /**
   * Internal setter that first checks for <code>System.property</code> default values.
   *
   * @param key key to set value for
   * @param defaultValue default value if not found
   */
  private void putWithSystemPropertyDefault(String key, String defaultValue) {
    properties.put(key, System.getProperty("paldb." + key, defaultValue));
  }

  /**
   * Internal getter.
   *
   * @param key key to get value for
   * @return value of null if not found
   */
  private String get(String key) {
    return properties.get(key);
  }

  /**
   * Internal contains.
   *
   * @param key key to test presence for
   * @return true if found, false otherwise
   */
  private boolean containsKey(String key) {
    return properties.containsKey(key);
  }

  /**
   * Gets the value for <code>key</code> or <code>defaultString</code> if not found.
   *
   * @param key key to get value for
   * @param defaultString default string if key not found
   * @return value of <code>defaultString</code> if not found
   */
  public String get(String key, String defaultString) {
    if (!containsKey(key)) {
      return defaultString;
    }
    return get(key);
  }

  /**
   * Sets the value for <code>key</code>.
   *
   * @param key key to set value for
   * @param value value
   * @return this configuration
   */
  public Configuration set(String key, String value) {
    checkReadOnly();

    properties.put(key, value);
    return this;
  }

  /**
   * Gets the boolean value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to get value for
   * @param defaultValue default value if key not found
   * @return value or <code>defaultValue</code> if not found
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    if (containsKey(key)) {
      return "true".equalsIgnoreCase(get(key));
    } else {
      return defaultValue;
    }
  }

  /**
   * Gets the boolean value <code>key</code>.
   *
   * @param key key to get value for
   * @throws java.lang.IllegalArgumentException if the key is not found
   * @return value
   */
  public boolean getBoolean(String key) {
    if (containsKey(key)) {
      return "true".equalsIgnoreCase(get(key));
    } else {
      throw new IllegalArgumentException("Missing key " + key + ".");
    }
  }

  /**
   * Gets the short value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to get value for
   * @param defaultValue default value if key not found
   * @return value or <code>defaultValue</code> if not found
   */
  public short getShort(String key, short defaultValue) {
    if (containsKey(key)) {
      return Short.parseShort(get(key));
    } else {
      return defaultValue;
    }
  }

  /**
   * Gets the short value <code>key</code>.
   *
   * @param key key to get value for
   * @throws java.lang.IllegalArgumentException if the key is not found
   * @return value
   */
  public short getShort(String key) {
    if (containsKey(key)) {
      return Short.parseShort(get(key));
    } else {
      throw new IllegalArgumentException("Missing key " + key + ".");
    }
  }

  /**
   * Gets the long value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to get value for
   * @param defaultValue default value if key not found
   * @return value or <code>defaultValue</code> if not found
   */
  public long getLong(String key, long defaultValue) {
    if (containsKey(key)) {
      return Long.parseLong(get(key));
    } else {
      return defaultValue;
    }
  }

  /**
   * Gets the long value <code>key</code>.
   *
   * @param key key to get value for
   * @throws java.lang.IllegalArgumentException if the key is not found
   * @return value
   */
  public long getLong(String key) {
    if (containsKey(key)) {
      return Long.parseLong(get(key));
    } else {
      throw new IllegalArgumentException("Missing key " + key + ".");
    }
  }

  /**
   * Gets the int value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to get value for
   * @param defaultValue default value if key not found
   * @return value or <code>defaultValue</code> if not found
   */
  public int getInt(String key, int defaultValue) {
    if (containsKey(key)) {
      return Integer.parseInt(get(key));
    } else {
      return defaultValue;
    }
  }

  /**
   * Gets the int value <code>key</code>.
   *
   * @param key key to get value for
   * @throws java.lang.IllegalArgumentException if the key is not found
   * @return value
   */
  public int getInt(String key) {
    if (containsKey(key)) {
      return Integer.parseInt(get(key));
    } else {
      throw new IllegalArgumentException("Missing key " + key + ".");
    }
  }

  /**
   * Gets the double value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to get value for
   * @param defaultValue default value if key not found
   * @return value or <code>defaultValue</code> if not found
   */
  public double getDouble(String key, double defaultValue) {
    if (containsKey(key)) {
      return Double.parseDouble(get(key));
    } else {
      return defaultValue;
    }
  }

  /**
   * Gets the double value <code>key</code>.
   *
   * @param key key to get value for
   * @throws java.lang.IllegalArgumentException if the key is not found
   * @return value
   */
  public double getDouble(String key) {
    if (containsKey(key)) {
      return Double.parseDouble(get(key));
    } else {
      throw new IllegalArgumentException("Missing key " + key + ".");
    }
  }

  /**
   * Gets the float value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to get value for
   * @param defaultValue default value if key not found
   * @return value or <code>defaultValue</code> if not found
   */
  public float getFloat(String key, float defaultValue) {
    if (containsKey(key)) {
      return Float.parseFloat(get(key));
    } else {
      return defaultValue;
    }
  }

  /**
   * Gets the float value <code>key</code>.
   *
   * @param key key to get value for
   * @throws java.lang.IllegalArgumentException if the key is not found
   * @return value
   */
  public float getFloat(String key) {
    if (containsKey(key)) {
      return Float.parseFloat(get(key));
    } else {
      throw new IllegalArgumentException("Missing key " + key + ".");
    }
  }

  /**
   * Gets the string list value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to get value for
   * @param defaultValue default value if key not found
   * @return value or <code>defaultValue</code> if not found
   */
  public List<String> getList(String key, List<String> defaultValue) {
    if (!containsKey(key)) {
      return defaultValue;
    }

    String value = get(key);
    String[] pieces = value.split("\\s*,\\s*");
    return Arrays.asList(pieces);
  }

  /**
   * Gets the string list value <code>key</code>.
   *
   * @param key key to get value for
   * @throws java.lang.IllegalArgumentException if the key is not found
   * @return value
   */
  public List<String> getList(String key) {
    if (!containsKey(key)) {
      throw new IllegalArgumentException("Missing key " + key + ".");
    }
    return getList(key, null);
  }

  /**
   * Gets the class value <code>key</code>.
   *
   * @param key key to get value for
   * @param <T> return type
   * @throws java.lang.IllegalArgumentException if the key is not found
   * @throws java.lang.ClassNotFoundException if the class can't be found
   * @return value
   */
  @SuppressWarnings("unchecked")
  public <T> Class<T> getClass(String key)
      throws ClassNotFoundException {
    if (containsKey(key)) {
      return (Class<T>) Class.forName(get(key));
    } else {
      throw new IllegalArgumentException("Missing key " + key + ".");
    }
  }

  /**
   * Register <code>serializer</code>.
   * <p>
   * The class for with the serializer is being registered is directly extracted from the class definition.
   *
   * @param serializer serializer to register
   */
  public void registerSerializer(Serializer serializer) {
    serializers.registerSerializer(serializer);
  }

  /**
   * Gets the serializer for the given class or null if not found.
   *
   * @param cls object class
   * @return serializer or null if not found
   */
  public Serializer getSerializer(Class cls) {
    return serializers.getSerializer(cls);
  }

  public Serializers getSerializers() {
    return serializers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Configuration that = (Configuration) o;

    if (!properties.equals(that.properties)) {
      return false;
    }
    if (!serializers.equals(that.serializers)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = properties != null ? properties.hashCode() : 0;
    result = 31 * result + (serializers != null ? serializers.hashCode() : 0);
    return result;
  }

  /**
   * Checks if the read-only flag is on and throws an exception.
   */
  private void checkReadOnly() {
    if (readOnly) {
      throw new UnsupportedOperationException(
          "The configuration values can't be set once the store reader/writer have been initialized");
    }
  }
}
