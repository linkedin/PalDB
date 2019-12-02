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

import java.io.*;


/**
 * Custom serializer for arbitrary Java classes.
 * <p>
 * Users should register serializers in the configuration:
 * <pre>
 * Configuration configuration = PalDB.newConfiguration();
 * configuration.registerKeySerializer(new PointSerializer());
 * configuration.registerValueSerializer(new PointSerializer());
 * </pre>
 *
 * @param <T> class type
 */
public interface Serializer<T> {

  /**
   * Writes the instance <code>input</code> to the data output.
   * @param input instance
   * @return serialized byte array
   * @throws IOException if an io error occurs
   */
  byte[] write(T input) throws IOException;

  /**
   * Reads the data input and creates the instance.
   *
   * @param bytes data input
   * @return new instance of type <code>K</code>.
   * @throws IOException if an io error occurs
   */
  T read(byte[] bytes) throws IOException;
}
