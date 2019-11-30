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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;


/**
 * Custom serializer for arbitrary Java classes.
 * <p>
 * Users should register serializers in the configuration:
 * <pre>
 * Configuration configuration = PalDB.newConfiguration();
 * configuration.registerSerializer(new PointSerializer());
 * </pre>
 *
 * @param <T> class type
 */
public interface Serializer<T> extends Serializable {

  /**
   * Writes the instance <code>input</code> to the data output.
   * @param dataOutput data output
   * @param input instance
   * @throws IOException if an io error occurs
   */
  void write(DataOutput dataOutput, T input) throws IOException;

  /**
   * Reads the data input and creates the instance.
   *
   * @param dataInput data input
   * @return new instance of type <code>K</code>.
   * @throws IOException if an io error occurs
   */
  T read(DataInput dataInput) throws IOException;
}
