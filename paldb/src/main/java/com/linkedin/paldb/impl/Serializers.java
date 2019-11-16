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

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Manages the custom serializers.
 */
public final class Serializers implements Serializable {

  // Logger
  private static final Logger log = LoggerFactory.getLogger(Serializers.class);
  private AtomicInteger counter;
  private Map<Class, SerializerWrapper> serializers;
  private Serializer[] serializersArray;

  /**
   * Default constructor.
   */
  public Serializers() {
    counter = new AtomicInteger();
    serializers = new HashMap<>();
    serializersArray = new Serializer[0];
  }

  /**
   * Registers the serializer.
   *
   * @param serializer serializer
   */
  public synchronized void registerSerializer(Serializer serializer) {
    Class objClass = getSerializerType(serializer);
    if (!serializers.containsKey(objClass)) {
      int index = counter.getAndIncrement();
      serializers.put(objClass, new SerializerWrapper(index, serializer));
      if (serializersArray.length <= index) {
        serializersArray = Arrays.copyOf(serializersArray, index + 1);
      }
      serializersArray[index] = serializer;

      log.info("Registered new serializer '{}' for '{}' at index {}", serializer.getClass().getName(),
              objClass.getName(), index);
    }
  }

  /**
   * Get the serializer instance associated with <code>cls</code> or null if not found.
   *
   * @param cls object class
   * @return serializer instance or null if not found
   */
  public <T> Serializer<T> getSerializer(Class<T> cls) {
    SerializerWrapper w = getSerializerWrapper(cls);
    if (w != null) {
      return w.serializer;
    }
    return null;
  }

  /**
   * Serializes this instance into an output stream.
   *
   * @param out data output
   * @throws IOException if an io error occurs
   */
  private void writeObject(ObjectOutputStream out)
      throws IOException {
    serialize(out, this);
  }

  /**
   * Serializes this class into a data output.
   *
   * @param out data output
   * @param serializers serializers instance
   * @throws IOException if an io error occurs
   */
  static void serialize(DataOutput out, Serializers serializers)
      throws IOException {
    StringBuilder msg = new StringBuilder(String.format("Serialize %d serializer classes:", serializers.serializers.values().size()));
    int size = serializers.serializers.values().size();

    out.writeInt(size);
    if (size > 0) {
      for (SerializerWrapper sw : serializers.serializers.values()) {
        int index = sw.index;
        String name = sw.serializer.getClass().getName();

        out.writeInt(index);
        out.writeUTF(name);

        msg.append(String.format("%n  (%d) %s", index, name));
      }
      log.info(msg.toString());
    }
  }

  /**
   * Deserializes this instance from an input stream.
   *
   * @param in data input
   * @throws IOException if an io error occurs
   * @throws ClassNotFoundException if a class error occurs
   */
  private void readObject(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    // Init
    counter = new AtomicInteger();
    serializers = new HashMap<>();
    serializersArray = new Serializer[0];

    deserialize(in, this);
  }

  /**
   * Deserializes this class from a data input.
   *
   * @param in data input
   * @param serializers serializers instance
   * @throws IOException if an io error occurs
   * @throws ClassNotFoundException if a class error occurs
   */
  static void deserialize(DataInput in, Serializers serializers)
      throws IOException, ClassNotFoundException {
    int size = in.readInt();
    if (size > 0) {
      StringBuilder msg = new StringBuilder(String.format("Deserialize %d serializer classes:", size));

      if (serializers.serializersArray.length < size) {
        serializers.serializersArray = Arrays.copyOf(serializers.serializersArray, size);
      }

      int max = 0;
      for (int i = 0; i < size; i++) {
        int index = in.readInt();
        max = Math.max(max, index);
        String serializerClassName = in.readUTF();
        try {
          Class<Serializer> serializerClass = (Class<Serializer>) Class.forName(serializerClassName);
          Serializer serializerInstance = serializerClass.newInstance();
          serializers.serializers
              .put(getSerializerType(serializerInstance), new SerializerWrapper(index, serializerInstance));
          serializers.serializersArray[index] = serializerInstance;

          msg.append(String.format("%n  (%d) %s", index, serializerClassName));
        } catch (Exception ex) {
          log.warn("Can't find the serializer '{}'", serializerClassName, ex);
        }
      }
      serializers.counter.set(max + 1);

      log.info(msg.toString());
    }
  }

  /**
   * Clear all serializers.
   */
  void clear() {
    log.info("Clear all serializers");

    serializers.clear();
    serializersArray = new Serializer[0];
    counter.set(0);
  }

  /**
   * Returns the serializer index associated with <code>cls</code>.
   *
   * @param cls object class
   * @return serializer index
   */
  int getIndex(Class cls) {
    return getSerializerWrapper(cls).index;
  }

  /**
   * Returns the serializer and its index associated with <code>cls</code>.
   *
   * @param cls object clas
   * @return serializer wrapper object
   */
  private SerializerWrapper getSerializerWrapper(Class cls) {
    SerializerWrapper w = serializers.get(cls);
    if (w != null) {
      return w;
    } else {
      // Try with interfaces implemented
      for (Class c : cls.getInterfaces()) {
        w = serializers.get(c);
        if (w != null) {
          return w;
        }
      }
    }
    return null;
  }

  /**
   * Returns the serializer given the index.
   *
   * @param index serializer index
   * @return serializer
   */
  Serializer getSerializer(int index) {
    if (index >= serializersArray.length) {
      throw new IllegalArgumentException(String.format("The serializer can't be found at index %d", index));
    }
    return serializersArray[index];
  }

  /**
   * Inner wrapper class that keeps the index attached to a serializer.
   */
  private static class SerializerWrapper implements Serializable {
    private int index;
    private Serializer serializer;

    /**
     * Used by deserialization.
     */
    public SerializerWrapper() {
    }

    public SerializerWrapper(int index, Serializer serializer) {
      this.index = index;
      this.serializer = serializer;
    }
  }

  /**
   * Returns the serializer's generic type.
   *
   * @param instance serializer instance
   * @return the class the serializer can serialize
   */
  private static Class<?> getSerializerType(Object instance) {
    Type type = instance.getClass().getGenericInterfaces()[0];
    if (type instanceof ParameterizedType) {
      Class<?> cls = null;
      Type clsType = ((ParameterizedType) type).getActualTypeArguments()[0];

      if (clsType instanceof GenericArrayType) {
        // Workaround for Java 6 (JDK bug 7151486)
        cls = Array.newInstance((Class) ((GenericArrayType) clsType).getGenericComponentType(), 0).getClass();
      } else {
        cls = (Class<?>) clsType;
      }

      if (Object.class.equals(cls)) {
        throw new RuntimeException("The serializer type can't be object");
      }
      return cls;
    } else {
      throw new RuntimeException(String
          .format("The serializer class %s is not generic or has an unknown type", instance.getClass().getName()));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Serializers that = (Serializers) o;

    if (serializers.size() != that.serializers.size()) {
      return false;
    }
    for (Map.Entry<Class, SerializerWrapper> entry : serializers.entrySet()) {
      SerializerWrapper sw = that.serializers.get(entry.getKey());
      if (sw == null || !sw.serializer.getClass().equals(entry.getValue().serializer.getClass())) {
        return false;
      }
    }

    return true;
  }
}
