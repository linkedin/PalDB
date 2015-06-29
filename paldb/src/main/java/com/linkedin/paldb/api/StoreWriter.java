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
public interface StoreWriter {

  /**
   * Close the store writer and append the data to the final destination. A
   * closed writer can't be reopened.
   */
  public void close();

  /**
   * Return the writer configuration. Configuration values should always be
   * set before calling the
   * <code>open()</code> method.
   *
   * @return the store configuration
   */
  public Configuration getConfiguration();

  /**
   * Put key-value to the store.
   *
   * @param key a key
   * @param value a value
   * @throws NullPointerException if <code>key</code> or <code>value</code> is
   * null
   */
  public void put(Object key, Object value);

  /**
   * Put multiple key-values to the store.
   *
   * @param keys a collection of keys
   * @param values a collection of values
   */
  public void putAll(Object[] keys, Object[] values);

  /**
   * Put serialized key-value entry to the store. <p> Use only this method if
   * you've already serialized the key and the value in their PalDB format.
   *
   * @param key a serialized key as a byte array
   * @param value a serialized value as a byte array
   * @throws NullPointerException if <code>key</code> or <code>value</code> is
   * null
   */
  public void put(byte[] key, byte[] value);
}
