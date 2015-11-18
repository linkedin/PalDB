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


/**
 * Main interface to read data from a PalDB store.
 * <p>
 * <code>PalDB.createReader()</code> method and then call the
 * <code>get()</code> method to fetch. Call the
 * <code>close()</code> to liberate resources when done.
 */
public interface StoreReader {

  /**
   * Closes the store reader and free resources.
   * <p>
   * A closed reader can't be reopened.
   */
  public void close();

  /**
   * Returns the reader's configuration.
   *
   * @return the store configuration
   */
  public Configuration getConfiguration();

  /**
   * Returns the store file.
   *
   * @return file
   */
  public File getFile();

  /**
   * Returns the number of keys in the store.
   *
   * @return key count
   */
  public long size();

  /**
   * Gets the value for <code>key</code> or null if not found.
   *
   * @param key key to fetch
   * @param <K> return type
   * @return value or null if not found
   */
  public <K> K get(Object key);

  /**
   * Gets the value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @param <K> return type
   * @return value of <code>defaultValue</code> if not found
   */
  public <K> K get(Object key, K defaultValue);

  /**
   * Gets the int value for <code>key</code>.
   *
   * @param key key to fetch
   * @return int value
   * @throws NotFoundException if not found
   */
  public int getInt(Object key)
      throws NotFoundException;

  /**
   * Gets the int value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return int value or <code>defaultValue</code> if not found
   */
  public int getInt(Object key, int defaultValue);

  /**
   * Gets the long value for <code>key</code>.
   *
   * @param key key to fetch
   * @return long value
   * @throws NotFoundException if not found
   */
  public long getLong(Object key)
      throws NotFoundException;

  /**
   * Gets the long value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return long value or <code>defaultValue</code> if not found
   */
  public long getLong(Object key, long defaultValue);

  /**
   * Gets the boolean value for <code>key</code>.
   *
   * @param key key to fetch
   * @return boolean value
   * @throws NotFoundException if not found
   */
  public boolean getBoolean(Object key)
      throws NotFoundException;

  /**
   * Gets the boolean value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return boolean value or <code>defaultValue</code> if not found
   */
  public boolean getBoolean(Object key, boolean defaultValue);

  /**
   * Gets the float value for <code>key</code>.
   *
   * @param key key to fetch
   * @return float value
   * @throws NotFoundException if not found
   */
  public float getFloat(Object key)
      throws NotFoundException;

  /**
   * Gets the float value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return float value or <code>defaultValue</code> if not found
   */
  public float getFloat(Object key, float defaultValue);

  /**
   * Gets the double value for <code>key</code>.
   *
   * @param key key to fetch
   * @return double value
   * @throws NotFoundException if not found
   */
  public double getDouble(Object key)
      throws NotFoundException;

  /**
   * Gets the double value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return double value or <code>defaultValue</code> if not found
   */
  public double getDouble(Object key, double defaultValue);

  /**
   * Gets the short value for <code>key</code>.
   *
   * @param key key to fetch
   * @return short value
   * @throws NotFoundException if not found
   */
  public short getShort(Object key)
      throws NotFoundException;

  /**
   * Gets the short value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return short value or <code>defaultValue</code> if not found
   */
  public short getShort(Object key, short defaultValue);

  /**
   * Gets the byte value for <code>key</code>.
   *
   * @param key key to fetch
   * @return byte value
   * @throws NotFoundException if not found
   */
  public byte getByte(Object key)
      throws NotFoundException;

  /**
   * Gets the byte value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return byte value or <code>defaultValue</code> if not found
   */
  public byte getByte(Object key, byte defaultValue);

  /**
   * Gets the string value for <code>key</code> or null if not found.
   *
   * @param key key to fetch
   * @return string value
   */
  public String getString(Object key);

  /**
   * Gets the string value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return string value or <code>defaultValue</code> if not found
   */
  public String getString(Object key, String defaultValue);

  /**
   * Gets the char value for <code>key</code>.
   *
   * @param key key to fetch
   * @return char value
   * @throws NotFoundException if not found
   */
  public char getChar(Object key)
      throws NotFoundException;

  /**
   * Gets the char value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return char value or <code>defaultValue</code> if not found
   */
  public char getChar(Object key, char defaultValue);

  /**
   * Gets the object array value for <code>key</code> or null if not found.
   *
   * @param key key to fetch
   * @param <K> return type
   * @return object array value or null if not found
   */
  public <K> K[] getArray(Object key);

  /**
   * Gets the object array value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @param <K> return type
   * @return object array value or <code>defaultValue</code> if not found
   */
  public <K> K[] getArray(Object key, K[] defaultValue);

  /**
   * Gets the int array value for <code>key</code>.
   *
   * @param key key to fetch
   * @return int array value
   * @throws NotFoundException if not found
   */
  public int[] getIntArray(Object key)
      throws NotFoundException;

  /**
   * Gets the int array value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return int array value or <code>defaultValue</code> if not found
   */
  public int[] getIntArray(Object key, int[] defaultValue);

  /**
   * Gets the long array value for <code>key</code>.
   *
   * @param key key to fetch
   * @return long array value
   * @throws NotFoundException if not found
   */
  public long[] getLongArray(Object key)
      throws NotFoundException;

  /**
   * Gets the long array value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return long array value or <code>defaultValue</code> if not found
   */
  public long[] getLongArray(Object key, long[] defaultValue);

  /**
   * Gets the boolean array value for <code>key</code>.
   *
   * @param key key to fetch
   * @return boolean array value
   * @throws NotFoundException if not found
   */
  public boolean[] getBooleanArray(Object key)
      throws NotFoundException;

  /**
   * Gets the boolean array value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return boolean array value or <code>defaultValue</code> if not found
   */
  public boolean[] getBooleanArray(Object key, boolean[] defaultValue);

  /**
   * Gets the float array value for <code>key</code>.
   *
   * @param key key to fetch
   * @return float array value
   * @throws NotFoundException if not found
   */
  public float[] getFloatArray(Object key)
      throws NotFoundException;

  /**
   * Gets the float array value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return float array value or <code>defaultValue</code> if not found
   */
  public float[] getFloatArray(Object key, float[] defaultValue);

  /**
   * Gets the double array value for <code>key</code>.
   *
   * @param key key to fetch
   * @return double array value
   * @throws NotFoundException if not found
   */
  public double[] getDoubleArray(Object key)
      throws NotFoundException;

  /**
   * Gets the double array value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return double array value or <code>defaultValue</code> if not found
   */
  public double[] getDoubleArray(Object key, double[] defaultValue);

  /**
   * Gets the short array value for <code>key</code>.
   *
   * @param key key to fetch
   * @return short array value
   * @throws NotFoundException if not found
   */
  public short[] getShortArray(Object key)
      throws NotFoundException;

  /**
   * Gets the short array value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return short array value or <code>defaultValue</code> if not found
   */
  public short[] getShortArray(Object key, short[] defaultValue);

  /**
   * Gets the byte array value for <code>key</code>.
   *
   * @param key key to fetch
   * @return byte array value
   * @throws NotFoundException if not found
   */
  public byte[] getByteArray(Object key)
      throws NotFoundException;

  /**
   * Gets the byte array value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return byte array value or <code>defaultValue</code> if not found
   */
  public byte[] getByteArray(Object key, byte[] defaultValue);

  /**
   * Gets the char array value for <code>key</code>.
   *
   * @param key key to fetch
   * @return char array value
   * @throws NotFoundException if not found
   */
  public char[] getCharArray(Object key)
      throws NotFoundException;

  /**
   * Gets the char array value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return char array value or <code>defaultValue</code> if not found
   */
  public char[] getCharArray(Object key, char[] defaultValue);

  /**
   * Gets the string array value for <code>key</code> or null if not found.
   *
   * @param key key to fetch
   * @return string array value or null if not found
   * @throws NotFoundException if not found
   */
  public String[] getStringArray(Object key)
      throws NotFoundException;

  /**
   * Gets the string array value for <code>key</code> or <code>defaultValue</code> if not found.
   *
   * @param key key to fetch
   * @param defaultValue default value
   * @return string array value or <code>defaultValue</code> if not found
   */
  public String[] getStringArray(Object key, String[] defaultValue);

  /**
   * Gets the store iterable.
   * <p>
   * Note that entry objects are reused.
   *
   * @param <K> key type
   * @param <V> value type
   * @return iterable over store
   */
  public <K, V> Iterable<Map.Entry<K, V>> iterable();

  /**
   * Gets the store keys iterable.
   *
   * @param <K> key type
   * @return iterable over keys
   */
  public <K> Iterable<K> keys();
}
