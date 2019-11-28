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

import com.linkedin.paldb.impl.StoreImpl;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * Factory for store readers and writers.
 * <p>
 * This class is the entry point to obtain {@link com.linkedin.paldb.api.StoreReader} and
 * {@link com.linkedin.paldb.api.StoreWriter} interfaces.
 */
public interface PalDB {

  /**
   * Creates a store reader from the specified <code>file</code> with a default configuration.
   * <p>
   * The file must exists.
   *
   * @param file a PalDB store file
   * @param <K> key type
   * @param <V> value type
   * @return a store reader
   */
  static <K,V> StoreReader<K,V> createReader(File file) {
    return StoreImpl.createReader(file, newConfiguration());
  }

  /**
   * Creates a store reader from the specified <code>file</code>.
   * <p>
   * The file must exists.
   *
   * @param file a PalDB store file
   * @param config configuration
   * @param <K> key type
   * @param <V> value type
   * @return a store reader
   */
  static <K,V> StoreReader<K,V> createReader(File file, Configuration<K,V> config) {
    return StoreImpl.createReader(file, config);
  }

  /**
   * Creates a store reader from the specified <code>stream</code>.
   * <p>
   * The reader will read the stream and write its content to a temporary file when this method is called. This is
   * specifically suited for stream coming from the JAR as a resource. The stream will be closed by this method.
   *
   * @param stream an input stream on a PalDB store file
   * @param config configuration
   * @param <K> key type
   * @param <V> value type
   * @return a store reader
   */
  static <K,V> StoreReader<K,V> createReader(InputStream stream, Configuration<K,V> config) {
    return StoreImpl.createReader(stream, config);
  }

  /**
   * Creates a store writer with the specified <code>file</code> as destination with a default configuration.
   * <p>
   * The parent folder is created if missing.
   *
   * @param file location of the output file
   * @param <K> key type
   * @param <V> value type
   * @return a store writer
   */
  static <K,V> StoreWriter<K,V> createWriter(File file) {
    return StoreImpl.createWriter(file, newConfiguration());
  }

  /**
   * Creates a store writer with the specified <code>file</code> as destination.
   * <p>
   * The parent folder is created if missing.
   *
   * @param file location of the output file
   * @param config configuration
   * @param <K> key type
   * @param <V> value type
   * @return a store writer
   */
  static <K,V> StoreWriter<K,V> createWriter(File file, Configuration<K,V> config) {
    return StoreImpl.createWriter(file, config);
  }

  /**
   * Creates a store writer with the specified  <code>stream</code> as destination.
   * <p>
   * The writer will only write bytes to the stream when {@link StoreWriter#close() }
   * is called.
   *
   * @param stream output stream
   * @param config configuration
   * @param <K> key type
   * @param <V> value type
   * @return a store writer
   */
  static <K,V> StoreWriter<K,V> createWriter(OutputStream stream, Configuration<K,V> config) {
    return StoreImpl.createWriter(stream, config);
  }

  /**
     * Creates a store reader-writer with the specified <code>file</code> as destination with a default configuration.
     * <p>
     * The parent folder is created if missing.
     *
     * @param file location of the output file
     * @param <K> key type
     * @param <V> value type
     * @return a store reader-writer
     */
  static <K,V> StoreRW<K,V> createRW(File file) {
        return StoreImpl.createRW(file, newConfiguration());
    }

  /**
     * Creates a store reader-writer with the specified <code>file</code> as destination.
     * <p>
     * The parent folder is created if missing.
     *
     * @param file location of the output file
     * @param config configuration
     * @param <K> key type
     * @param <V> value type
     * @return a store reader-writer
     */
  static <K,V> StoreRW<K,V> createRW(File file, Configuration<K,V> config) {
    return StoreImpl.createRW(file, config);
  }

  /**
   * Creates new configuration with default values.
   *
   * @return new configuration
   */
  static <K,V> Configuration<K,V> newConfiguration() {
    return new Configuration<>();
  }
}
