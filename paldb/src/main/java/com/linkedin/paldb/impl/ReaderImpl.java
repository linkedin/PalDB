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
import com.linkedin.paldb.utils.DataInputOutput;
import org.slf4j.*;

import java.io.*;
import java.util.*;


/**
 * Store reader implementation.
 */
public final class ReaderImpl<K,V> implements StoreReader<K,V> {

  // Logger
  private static final Logger log = LoggerFactory.getLogger(ReaderImpl.class);
  // Configuration
  private final Configuration config;
  // Buffer
  private final DataInputOutput dataInputOutput = new DataInputOutput();
  // Storage
  private final StorageReader storage;
  // Serialization
  private final StorageSerialization serialization;
  // Cache
  private final StorageCache<K,V> cache;
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
      log.info("Opening reader storage");
      serialization = new StorageSerialization(config);
      storage = new StorageReader(config, file);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    opened = true;

    // Cache
    cache = StorageCache.initCache(config);
  }

  @Override
  public void close() {
    checkOpen();
    try {
      log.info("Closing reader storage");
      storage.close();
      opened = false;
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
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
  public V get(K key) {
    return get(key, null);
  }

  @Override
  public V get(K key, V defaultValue) {
    checkOpen();
    if (key == null) {
      throw new NullPointerException("The key can't be null");
    }

    try {
      byte[] valueBytes = storage.get(serialization.serializeKey(key));
      if (valueBytes != null) {
        return (V) serialization.deserialize(new DataInputOutput(valueBytes));
      } else {
        return defaultValue;
      }
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<Map.Entry<K, V>> iterable() {
    checkOpen();
    return new ReaderIterable<>(storage, serialization);
  }

  @Override
  public Iterator<Map.Entry<K,V>> iterator() {
      return iterable().iterator();
  }

  @Override
  public Iterable<K> keys() {
    checkOpen();
    return new ReaderKeyIterable<>(storage, serialization);
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
