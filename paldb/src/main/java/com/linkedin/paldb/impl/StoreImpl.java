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
import com.linkedin.paldb.api.StoreReader;
import com.linkedin.paldb.api.StoreWriter;
import com.linkedin.paldb.utils.TempUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Static implementation factory.
 */
public final class StoreImpl {

  private final static Logger LOGGER = Logger.getLogger(StoreImpl.class.getName());

  private StoreImpl() {
  }

  public static StoreReader createReader(File file, Configuration config) {
    if (file == null || config == null) {
      throw new NullPointerException();
    }
    LOGGER.log(Level.INFO, "Initialize reader from file {0}", file.getName());
    return new ReaderImpl(config, file);
  }

  public static StoreReader createReader(InputStream stream, Configuration config) {
    if (stream == null || config == null) {
      throw new NullPointerException();
    }
    LOGGER.log(Level.INFO, "Initialize reader from stream, copying into temp folder");
    try {
      File file = TempUtils.copyIntoTempFile("paldbtempreader", stream);
      LOGGER.log(Level.INFO, "Copied stream into temp file {0}", file.getName());
      return new ReaderImpl(config, file);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static StoreWriter createWriter(File file, Configuration config) {
    if (file == null || config == null) {
      throw new NullPointerException();
    }
    try {
      LOGGER.log(Level.INFO, "Initialize writer from file {0}", file.getName());
      File parent = file.getParentFile();
      if (parent != null && !parent.exists()) {
        if (parent.mkdirs()) {
          LOGGER.log(Level.INFO, "Creating directories for path {0}", file.getName());
        } else {
          throw new RuntimeException(String.format("Couldn't create directory %s", parent));
        }
      }
      return new WriterImpl(config, file);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static StoreWriter createWriter(OutputStream stream, Configuration config) {
    if (stream == null || config == null) {
      throw new NullPointerException();
    }
    LOGGER.info("Initialize writer from stream");
    return new WriterImpl(config, stream);
  }
}
