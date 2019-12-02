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

/**
 * Main interface to write data to a PalDB store.
 * <p>
 * Users of this class should initialize it by using the
 * <code>PalDB.createWriter()</code> method and then call the
 * <code>put()</code> method to insert. Call the
 * <code>close()</code> to liberate resources when done.
 * <p>
 * Note that duplicates aren't allowed.
 */
public interface StoreWriter<K,V> extends AutoCloseable {

  /**
   * Close the store writer and append the data to the final destination. A
   * closed writer can't be reopened.
   */
  @Override
  void close();

  /**
   * Return the writer configuration. Configuration values should always be
   * set before calling the
   * <code>open()</code> method.
   *
   * @return the store configuration
   */
  Configuration<K,V> getConfiguration();

  /**
   * Put key-value to the store.
   *
   * @param key a key
   * @param value a value
   * @throws NullPointerException if <code>key</code> or <code>value</code> is
   * null
   */
  void put(K key, V value);

  /**
   * Put multiple key-values to the store.
   *
   * @param keys a collection of keys
   * @param values a collection of values
   */
  default void putAll(K[] keys, V[] values) {
    if (keys == null || values == null) {
      throw new NullPointerException();
    }
    if (keys.length != values.length) {
      throw new IllegalArgumentException("Key and value collections should be the same size");
    }
    int size = keys.length;
    for (int i = 0; i < size; i++) {
      put(keys[i], values[i]);
    }
  }

  /**
   * Put serialized key-value entry to the store. <p> Use only this method if
   * you've already serialized the key and the value in their PalDB format.
   *
   * @param key a serialized key as a byte array
   * @param value a serialized value as a byte array
   * @throws NullPointerException if <code>key</code> or <code>value</code> is
   * null
   */
  void put(byte[] key, byte[] value);

  /**
   * Removes key from the store
   * @param key Key to be removed
   */
  void remove(K key);
}
