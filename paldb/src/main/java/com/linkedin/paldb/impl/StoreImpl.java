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
import com.linkedin.paldb.utils.TempUtils;
import org.slf4j.*;

import java.io.*;


/**
 * Static implementation factory.
 */
public final class StoreImpl {

  private static final Logger log = LoggerFactory.getLogger(StoreImpl.class);

  private StoreImpl() {
  }

  public static StoreReader createReader(File file, Configuration config) {
    if (file == null || config == null) {
      throw new NullPointerException();
    }
    log.info("Initialize reader from file {}", file.getName());
    return new ReaderImpl(config, file);
  }

  public static StoreReader createReader(InputStream stream, Configuration config) {
    if (stream == null || config == null) {
      throw new NullPointerException();
    }
    log.info("Initialize reader from stream, copying into temp folder");
    try {
      File file = TempUtils.copyIntoTempFile("paldbtempreader", stream);
      log.info("Copied stream into temp file {}", file.getName());
      return new ReaderImpl(config, file);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  public static StoreWriter createWriter(File file, Configuration config) {
    if (file == null || config == null) {
      throw new NullPointerException();
    }
    try {
      log.info("Initialize writer from file {}", file.getName());
      File parent = file.getParentFile();
      if (parent != null && !parent.exists()) {
        if (parent.mkdirs()) {
          log.info("Creating directories for path {}", file.getName());
        } else {
          throw new RuntimeException(String.format("Couldn't create directory %s", parent));
        }
      }
      return new WriterImpl(config, file);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  public static StoreWriter createWriter(OutputStream stream, Configuration config) {
    if (stream == null || config == null) {
      throw new NullPointerException();
    }
    log.info("Initialize writer from stream");
    return new WriterImpl(config, stream);
  }
}
