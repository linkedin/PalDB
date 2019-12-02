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

import java.io.File;
import java.util.Map;
import java.util.stream.Stream;


/**
 * Main interface to read data from a PalDB store.
 * <p>
 * <code>PalDB.createReader()</code> method and then call the
 * <code>get()</code> method to fetch. Call the
 * <code>close()</code> to liberate resources when done.
 */
public interface StoreReader<K,V> extends AutoCloseable {

  /**
   * Closes the store reader and free resources.
   * <p>
   * A closed reader can't be reopened.
   */
  @Override
  void close();

  /**
   * Returns the reader's configuration.
   *
   * @return the store configuration
   */
  Configuration<K,V> getConfiguration();

  /**
   * Returns the store file.
   *
   * @return file
   */
  File getFile();

  /**
   * Returns the number of keys in the store.
   *
   * @return key count
   */
  long size();

  /**
   * Gets the value for <code>key</code> or null if not found.
   *
   * @param key key to fetch
   * @return value or null if not found
   */
  default V get(K key) {
    return get(key, null);
  }

  /**
   * Gets the value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return value of <code>defaultValue</code> if not found
   */
  V get(K key, V defaultValue);

  /**
   * Streams all database entries.
   * Stream should be properly closed, using try-with-resources.
   * @return stream of Map.Entry<K,V>
   */
  Stream<Map.Entry<K,V>> stream();

  /**
   * Streams all database keys.
   * Stream should be properly closed, using try-with-resources.
   * @return stream of keys
   */
  Stream<K> streamKeys();
}
