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
import org.slf4j.*;

import java.io.*;


/**
 * Store writer implementation.
 */
public final class WriterImpl<K,V> implements StoreWriter<K,V> {

  // Logger
  private static final Logger log = LoggerFactory.getLogger(WriterImpl.class);
  // Configuration
  private final Configuration<K,V> config;
  // Storage
  private final StorageWriter storage;
  // Serialization
  private final StorageSerialization<K,V> serialization;
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
  WriterImpl(Configuration<K,V> config, File file) throws IOException {
    this(config, new FileOutputStream(file), file);
  }

  /**
   * Stream constructor.
   *
   * @param config configuration
   * @param stream input stream
   */
  WriterImpl(Configuration<K,V> config, OutputStream stream) {
    this(config, stream, null);
  }

  /**
   * Private constructor.
   *
   * @param config configuration
   * @param stream output stream
   */
  private WriterImpl(Configuration<K,V> config, OutputStream stream, File file) {
    this.config = config;
    this.outputStream = stream;
    this.file = file;

    // Open storage
    log.debug("Opening writer storage");
    serialization = new StorageSerialization<>(config);
    storage = new StorageWriter(config, outputStream);
    opened = true;
  }

  @Override
  public void close() {
    checkOpen();
    try {
      if (file != null) {
        log.info("Closing writer storage, writing to file at {}", file.getAbsolutePath());
      } else {
        log.info("Closing writer storage, writing to stream");
      }

      storage.close();
      outputStream.close();
      opened = false;
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public Configuration<K,V> getConfiguration() {
    return config;
  }

  @Override
  public void put(K key, V value) {
    checkOpen();
    if (key == null) {
      throw new NullPointerException();
    }
    try {
      byte[] keyBytes = serialization.serializeKey(key);
      storage.put(keyBytes, serialization.serializeValue(value));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
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
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public void remove(K key) {
    checkOpen();
    if (key == null) {
      throw new NullPointerException();
    }
    try {
      byte[] keyBytes = serialization.serializeKey(key);
      storage.put(keyBytes, null);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  // UTILITIES

  private void checkOpen() {
    if (!opened) {
      throw new IllegalStateException("The store is closed");
    }
  }
}
