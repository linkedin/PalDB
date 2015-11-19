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
import com.linkedin.paldb.api.NotFoundException;
import com.linkedin.paldb.api.StoreReader;
import com.linkedin.paldb.utils.DataInputOutput;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Store reader implementation.
 */
public final class ReaderImpl implements StoreReader {

  // Logger
  private final static Logger LOGGER = Logger.getLogger(ReaderImpl.class.getName());
  // Configuration
  private final Configuration config;
  // Buffer
  private final DataInputOutput dataInputOutput = new DataInputOutput();
  // Storage
  private final StorageReader storage;
  // Serialization
  private final StorageSerialization serialization;
  // Cache
  private final StorageCache cache;
  // File
  private final File file;
  // Opened?
  private boolean opened;

  /**
   * Private constructor.
   *
   * @param config configuration
   * @param file store file
   */
  ReaderImpl(Configuration config, File file) {
    this.config = config;
    this.file = file;

    // Open storage
    try {
      LOGGER.log(Level.INFO, "Opening reader storage");
      serialization = new StorageSerialization(config);
      storage = new StorageReader(config, file);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    opened = true;

    // Cache
    cache = StorageCache.initCache(config);
  }

  @Override
  public void close() {
    checkOpen();
    try {
      LOGGER.log(Level.INFO, "Closing reader storage");
      storage.close();
      opened = false;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public long size() {
    checkOpen();
    return storage.getKeyCount();
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  @Override
  public File getFile() {
    return file;
  }

  @Override
  public <K> K get(Object key) {
    return (K) get(key, null);
  }

  @Override
  public <K> K get(Object key, K defaultValue) {
    checkOpen();
    if (key == null) {
      throw new NullPointerException("The key can't be null");
    }
    K value = cache.get(key);
    if (value == null) {
      try {
        byte[] valueBytes = storage.get(serialization.serializeKey(key));
        if (valueBytes != null) {

          Object v = serialization.deserialize(dataInputOutput.reset(valueBytes));
          cache.put(key, v);
          return (K) v;
        } else {
          return defaultValue;
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    } else if (value == StorageCache.NULL_VALUE) {
      return null;
    }
    return value;
  }

  @Override
  public int getInt(Object key, int defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public int getInt(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return ((Integer) val).intValue();
  }

  @Override
  public long getLong(Object key, long defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public long getLong(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return ((Long) val).longValue();
  }

  @Override
  public boolean getBoolean(Object key, boolean defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public boolean getBoolean(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return ((Boolean) val).booleanValue();
  }

  @Override
  public float getFloat(Object key, float defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public float getFloat(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return ((Float) val).floatValue();
  }

  @Override
  public double getDouble(Object key, double defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public double getDouble(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return ((Double) val).intValue();
  }

  @Override
  public short getShort(Object key, short defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public short getShort(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return ((Short) val).shortValue();
  }

  @Override
  public byte getByte(Object key, byte defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public byte getByte(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return ((Byte) val).byteValue();
  }

  @Override
  public String getString(Object key, String defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public String getString(Object key) {
    return (String) get(key, null);
  }

  @Override
  public char getChar(Object key, char defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public char getChar(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return ((Character) val).charValue();
  }

  @Override
  public <K> K[] getArray(Object key) {
    return (K[]) get(key, null);
  }

  @Override
  public <K> K[] getArray(Object key, K[] defaultValue) {
    return (K[]) get(key, defaultValue);
  }

  @Override
  public int[] getIntArray(Object key, int[] defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public int[] getIntArray(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return (int[]) val;
  }

  @Override
  public long[] getLongArray(Object key, long[] defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public long[] getLongArray(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return (long[]) val;
  }

  @Override
  public boolean[] getBooleanArray(Object key, boolean[] defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public boolean[] getBooleanArray(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return (boolean[]) val;
  }

  @Override
  public float[] getFloatArray(Object key, float[] defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public float[] getFloatArray(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return (float[]) val;
  }

  @Override
  public double[] getDoubleArray(Object key, double[] defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public double[] getDoubleArray(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return (double[]) val;
  }

  @Override
  public short[] getShortArray(Object key, short[] defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public short[] getShortArray(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return (short[]) val;
  }

  @Override
  public byte[] getByteArray(Object key, byte[] defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public byte[] getByteArray(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return (byte[]) val;
  }

  @Override
  public String[] getStringArray(Object key, String[] defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public String[] getStringArray(Object key) {
    return (String[]) get(key, null);
  }

  @Override
  public char[] getCharArray(Object key, char[] defaultValue) {
    return get(key, defaultValue);
  }

  @Override
  public char[] getCharArray(Object key)
      throws NotFoundException {
    Object val = get(key);
    if (val == null) {
      throw new NotFoundException(key);
    }
    return (char[]) val;
  }

  @Override
  public <K, V> Iterable<Map.Entry<K, V>> iterable() {
    checkOpen();
    return new ReaderIterable(storage, serialization);
  }

  @Override
  public <K> Iterable<K> keys() {
    checkOpen();
    return new ReaderKeyIterable<K>(storage, serialization);
  }

  // UTILITIES

  /**
   * Checks if the store is open and throws an exception otherwise.
   */
  private void checkOpen() {
    if (!opened) {
      throw new IllegalStateException("The store is closed");
    }
  }
}
