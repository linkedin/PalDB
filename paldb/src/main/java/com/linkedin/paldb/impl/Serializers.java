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

import java.util.*;


/**
 * Manages the custom serializers.
 */
public final class Serializers {

  // Logger
  private static final Logger log = LoggerFactory.getLogger(Serializers.class);
  private final Map<String, Serializer<?>> serializerMap;

  /**
   * Default constructor.
   */
  public Serializers() {
    serializerMap = new HashMap<>();
  }

  /**
   * Registers the serializer.
   *
   * @param serializer serializer
   */
  public synchronized <T> void registerSerializer(Serializer<T> serializer) {
    var className = serializer.serializedClass().getName();
    serializerMap.putIfAbsent(className, serializer);
    log.info("Registered new serializer '{}' for '{}'", serializer.getClass().getName(), className);
  }

  /**
   * Get the serializer instance associated with <code>cls</code> or null if not found.
   *
   * @param cls object class
   * @return serializer instance or null if not found
   */
  public Serializer getSerializer(Class<?> cls) {
    return getSerializer(cls.getName());
}

  public Serializer getSerializer(String className) {
    return serializerMap.get(className);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Serializers)) return false;
    final Serializers that = (Serializers) o;
    return Objects.equals(serializerMap, that.serializerMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serializerMap);
  }
}
