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
import com.linkedin.paldb.api.StoreWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Store writer implementation.
 */
public final class WriterImpl implements StoreWriter {

  // Logger
  private final static Logger LOGGER = Logger.getLogger(WriterImpl.class.getName());
  // Configuration
  private final Configuration config;
  // Storage
  private final StorageWriter storage;
  // Serialization
  private final StorageSerialization serialization;
  // File (can be null)
  private final File file;
  // Stream
  private final OutputStream outputStream;
  // Opened?
  private boolean opened;

  /**
   * File constructor.
   *
   * @param config configuration
   * @param file input file
   */
  WriterImpl(Configuration config, File file)
      throws IOException {
    this(config, new FileOutputStream(file), file);
  }

  /**
   * Stream constructor.
   *
   * @param config configuration
   * @param stream input stream
   */
  WriterImpl(Configuration config, OutputStream stream) {
    this(config, stream, null);
  }

  /**
   * Private constructor.
   *
   * @param config configuration
   * @param stream output stream
   */
  private WriterImpl(Configuration config, OutputStream stream, File file) {
    this.config = config;
    this.outputStream = stream;
    this.file = file;

    // Open storage
    LOGGER.log(Level.INFO, "Opening writer storage");
    serialization = new StorageSerialization(config);
    storage = new StorageWriter(config, outputStream);
    opened = true;
  }

  @Override
  public void close() {
    checkOpen();
    try {
      if (file != null) {
        LOGGER.log(Level.INFO, "Closing writer storage, writing to file at " + file.getAbsolutePath());
      } else {
        LOGGER.log(Level.INFO, "Closing writer storage, writing to stream");
      }

      storage.close();
      outputStream.close();
      opened = false;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Configuration getConfiguration() {
    return config;
  }

  @Override
  public void put(Object key, Object value) {
    checkOpen();
    if (key == null) {
      throw new NullPointerException();
    }
    try {
      byte[] keyBytes = serialization.serializeKey(key);
      storage.put(keyBytes, serialization.serializeValue(value));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void putAll(Object[] keys, Object[] values) {
    checkOpen();
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

  @Override
  public void put(byte[] key, byte[] value) {
    checkOpen();
    if (key == null || value == null) {
      throw new NullPointerException();
    }
    try {
      storage.put(key, value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  // UTILITIES

  private void checkOpen() {
    if (!opened) {
      throw new IllegalStateException("The store is closed");
    }
  }
}
