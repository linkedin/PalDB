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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Manages the custom serializers.
 */
public final class Serializers implements Serializable {

  // Logger
  private final static Logger LOGGER = Logger.getLogger(Serializers.class.getName());
  private AtomicInteger COUNTER;
  private Map<Class, SerializerWrapper> serializers;
  private Serializer[] serializersArray;

  /**
   * Default constructor.
   */
  public Serializers() {
    COUNTER = new AtomicInteger();
    serializers = new HashMap<Class, SerializerWrapper>();
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
      int index = COUNTER.getAndIncrement();
      serializers.put(objClass, new SerializerWrapper(index, serializer));
      if (serializersArray.length <= index) {
        serializersArray = Arrays.copyOf(serializersArray, index + 1);
      }
      serializersArray[index] = serializer;

      LOGGER.info(String
          .format("Registered new serializer '%s' %n  for '%s' at index %d", serializer.getClass().getName(),
              objClass.getName(), index));
    }
  }

  /**
   * Get the serializer instance associated with <code>cls</code> or null if not found.
   *
   * @param cls object class
   * @return serializer instance or null if not found
   */
  public Serializer getSerializer(Class cls) {
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
      LOGGER.info(msg.toString());
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
    COUNTER = new AtomicInteger();
    serializers = new HashMap<Class, SerializerWrapper>();
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
          LOGGER.log(Level.WARNING, (String.format("Can't find the serializer '%s'", serializerClassName)), ex);
        }
      }
      serializers.COUNTER.set(max + 1);

      LOGGER.info(msg.toString());
    }
  }

  /**
   * Clear all serializers.
   */
  void clear() {
    LOGGER.info("Clear all serializers");

    serializers.clear();
    serializersArray = new Serializer[0];
    COUNTER.set(0);
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
