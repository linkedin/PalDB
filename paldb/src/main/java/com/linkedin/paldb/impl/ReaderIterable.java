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

import com.linkedin.paldb.utils.DataInputOutput;
import java.util.Iterator;
import java.util.Map;


/**
 * Store iterable that return store iterator.
 */
public final class ReaderIterable<K, V> implements Iterable<Map.Entry<K, V>> {

  // Iterable
  private Iterable<Map.Entry<byte[], byte[]>> byteIterable;

  // Serialization
  private StorageSerialization serialization;

  /**
   * Constructor.
   *
   * @param byteIterable byte iterator
   * @param serialization serialization
   */
  ReaderIterable(Iterable<Map.Entry<byte[], byte[]>> byteIterable, StorageSerialization serialization) {
    this.byteIterable = byteIterable;
    this.serialization = serialization;
  }

  @Override
  public Iterator<Map.Entry<K, V>> iterator() {
    return new ReaderIterator<K, V>(byteIterable.iterator(), serialization);
  }

  /**
   * Store iterator that streams deserialized key/value entries.
   * <p>
   * Note that entry objects are reused.
   */
  private static final class ReaderIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    // Reusable entry
    private final FastEntry<K, V> entry = new FastEntry<K, V>();
    // Iterator
    private final Iterator<Map.Entry<byte[], byte[]>> byteIterator;
    // Buffer
    private final DataInputOutput dataInputOutput = new DataInputOutput();
    // Serialization
    private StorageSerialization serialization;

    /**
     * Constructor.
     *
     * @param byteIterator byte iterator
     * @param serialization serialization
     */
    ReaderIterator(Iterator<Map.Entry<byte[], byte[]>> byteIterator, StorageSerialization serialization) {
      this.byteIterator = byteIterator;
      this.serialization = serialization;
    }

    @Override
    public boolean hasNext() {
      return byteIterator.hasNext();
    }

    @Override
    public Map.Entry<K, V> next() {
      Map.Entry<byte[], byte[]> byteEntry = byteIterator.next();
      try {
        K key = (K) serialization.deserialize(dataInputOutput.reset(byteEntry.getKey()));
        V value = (V) serialization.deserialize(dataInputOutput.reset(byteEntry.getValue()));
        entry.set(key, value);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported.");
    }

    /**
     * Reusable <code>Map.Entry</code>.
     */
    private static class FastEntry<K, V> implements Map.Entry<K, V> {

      private K key;
      private V val;

      /**
       * Sets the key/value
       * @param k key
       * @param v value
       */
      protected void set(K k, V v) {
        this.key = k;
        this.val = v;
      }

      @Override
      public K getKey() {
        return key;
      }

      @Override
      public V getValue() {
        return val;
      }

      @Override
      public Object setValue(Object value) {
        throw new UnsupportedOperationException("Not supported.");
      }
    }
  }
}
