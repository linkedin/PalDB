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

package com.linkedin.paldb.utils;

import java.io.IOException;
import java.util.Arrays;


/**
 * Enum that represents the version of the data format.
 * <p>
 * The format version ensures compatibility between the writer and the reader (i.e. should be equal).
 */
public enum FormatVersion {
  PALDB_V1;

  /**
   * Returns true if <code>fv</code> is equals to <code>this</code>.
   *
   * @param fv a format version
   * @return true if <code>fv</code> is equals to <code>this</code> enum
   */
  public boolean is(FormatVersion fv) {
    return fv.equals(this);
  }

  /**
   * Return the byte representation of this format version.
   *
   * @return format version byte representation
   */
  public byte[] getBytes() {
    try {
      DataInputOutput dio = new DataInputOutput();
      dio.writeUTF(this.name());
      byte[] res = dio.toByteArray();
      return Arrays.copyOfRange(res, 1, res.length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get a version based on bytes or return null if not found.
   *
   * @param bytes byte representation
   * @return format version or null if not found
   */
  public static FormatVersion fromBytes(byte[] bytes) {
    String version = null;
    try {
      byte[] withSize = new byte[bytes.length + 1];
      withSize[0] = (byte) bytes.length;
      System.arraycopy(bytes, 0, withSize, 1, bytes.length);
      DataInputOutput dio = new DataInputOutput(withSize);
      version = dio.readUTF();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      return FormatVersion.valueOf(version);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Returns the byte representation of the version prefix.
   *
   * @return prefix byte representation
   */
  public static byte[] getPrefixBytes() {
    try {
      DataInputOutput dio = new DataInputOutput();
      dio.writeUTF("PALDB");
      byte[] res = dio.toByteArray();
      return Arrays.copyOfRange(res, 1, res.length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return the latest version of the format.
   *
   * @return the latest format version
   */
  public static FormatVersion getLatestVersion() {
    return FormatVersion.values()[FormatVersion.values().length - 1];
  }
}
