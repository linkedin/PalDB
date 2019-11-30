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

import com.linkedin.paldb.api.Serializer;
import org.slf4j.*;

import java.util.Objects;


/**
 * Manages the custom serializers.
 */
public final class Serializers<K,V> {

  // Logger
  private static final Logger log = LoggerFactory.getLogger(Serializers.class);

  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;

  /**
   * Default constructor.
   */
  public Serializers() {
    this.keySerializer = null;
    this.valueSerializer = null;
  }

  /**
   * Registers keu serializer.
   *
   * @param keySerializer serializer
   */
  public synchronized void registerKeySerializer(Serializer<K> keySerializer) {
    this.keySerializer = keySerializer;
    log.info("Registered new key serializer '{}'", keySerializer.getClass().getName());
  }

  /**
   * Register value serializer
   * @param valueSerializer serializer
   */
  public synchronized void registerValueSerializer(Serializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
    log.info("Registered new value serializer '{}'", valueSerializer.getClass().getName());
  }

  public Serializer<K> keySerializer() {
    return keySerializer;
  }

  public Serializer<V> valueSerializer() {
    return valueSerializer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final Serializers<?, ?> that = (Serializers<?, ?>) o;
    return Objects.equals(keySerializer, that.keySerializer) &&
            Objects.equals(valueSerializer, that.valueSerializer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keySerializer, valueSerializer);
  }
}
