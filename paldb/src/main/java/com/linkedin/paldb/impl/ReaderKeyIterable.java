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
 * Store iterable that return store key iterator.
 */
public final class ReaderKeyIterable<K> implements Iterable<K> {

  // Iterable
  private Iterable<Map.Entry<byte[], byte[]>> byteIterable;

  // Serialization
  private StorageSerialization serialization;

  /**
   * Constructor.
   *
   * @param byteIterable  byte iterator
   * @param serialization serialization
   */
  ReaderKeyIterable(Iterable<Map.Entry<byte[], byte[]>> byteIterable, StorageSerialization serialization) {
    this.byteIterable = byteIterable;
    this.serialization = serialization;
  }

  @Override
  public Iterator<K> iterator() {
    return new ReaderKeyIterator<K>(byteIterable.iterator(), serialization);
  }

  /**
   * Store key iterator that streams deserialized keys.
   */
  private static final class ReaderKeyIterator<K> implements Iterator<K> {

    // Iterator
    private final Iterator<Map.Entry<byte[], byte[]>> byteIterator;
    // Buffer
    private final DataInputOutput dataInputOutput = new DataInputOutput();
    // Serialization
    private StorageSerialization serialization;

    /**
     * Constructor.
     *
     * @param byteIterator  byte iterator
     * @param serialization serialization
     */
    ReaderKeyIterator(Iterator<Map.Entry<byte[], byte[]>> byteIterator, StorageSerialization serialization) {
      this.byteIterator = byteIterator;
      this.serialization = serialization;
    }

    @Override
    public boolean hasNext() {
      return byteIterator.hasNext();
    }

    @Override
    public K next() {
      Map.Entry<byte[], byte[]> byteEntry = byteIterator.next();
      try {
        K key = (K) serialization.deserialize(dataInputOutput.reset(byteEntry.getKey()));
        return key;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported.");
    }
  }
}
