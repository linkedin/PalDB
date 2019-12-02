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


import com.linkedin.paldb.impl.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;


class TestConfiguration {

  @Test
  void testConfiguration() {
    var c = new Configuration<>();
    c.set("foo", "bar");
    assertEquals("bar", c.get("foo", null));
    assertEquals("foo", c.get("bar", "foo"));
  }

  @Test
  void testConfigurationCopy() {
    var c = new Configuration<>();
    c.set("foo", "bar");

    Configuration r = new Configuration(c);
    assertEquals("bar", r.get("foo", null));

    c.set("foo", "");
    assertEquals("bar", r.get("foo", null));
  }

  @Test
  void testConfigurationReadOnly() {
    var c = new Configuration<>();
    c.set("foo", "bar");

    assertThrows(UnsupportedOperationException.class, () -> {
      Configuration r = new Configuration(c);
      r.set("foo", "bar");
    });
  }

  @Test
  void testEqualsEmpty() {
    assertEquals(new Configuration(), new Configuration());
  }

  @Test
  void testEquals() {
    Configuration c1 = new Configuration();
    c1.set("foo", "bar");

    Configuration c2 = new Configuration();
    c2.set("foo", "bar");

    Configuration c3 = new Configuration();
    c3.set("foo", "notbar");

    assertEquals(c1, c2);
    assertEquals(c1, c1);
    assertNotEquals("any value", c1);
    assertNotEquals(c1, c3);
    assertEquals(c1.hashCode(), c2.hashCode());
  }

  @Test
  void testGetBoolean() {
    var c = new Configuration<>();
    c.set("foo", "true");
    c.set("bar", "false");

    assertTrue(c.getBoolean("foo"));
    assertFalse(c.getBoolean("bar"));
  }

  @Test
  void testGetBooleanMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getBoolean("foo"));
  }

  @Test
  void testGetBooleanDefault() {
    var c = new Configuration<>();
    c.set("foo", "true");

    assertTrue(c.getBoolean("foo", false));
    assertTrue(c.getBoolean("bar", true));
  }

  @Test
  void testGetDouble() {
    var c = new Configuration<>();
    c.set("foo", "1.0");

    assertEquals(1.0, c.getDouble("foo"));
  }

  @Test
  void testGetDoubleMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getDouble("foo"));
  }

  @Test
  void testGetDoubleDefault() {
    var c = new Configuration<>();
    c.set("foo", "1.0");

    assertEquals(1.0, c.getDouble("foo", 2.0));
    assertEquals(2.0, c.getDouble("bar", 2.0));
  }

  @Test
  void testGetFloat() {
    var c = new Configuration<>();
    c.set("foo", "1.0");

    assertEquals(1f, c.getFloat("foo"));
  }

  @Test
  void testGetFloatMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getFloat("foo"));
  }

  @Test
  void testGetFloatDefault() {
    var c = new Configuration<>();
    c.set("foo", "1.0");

    assertEquals(1f, c.getFloat("foo", 2f));
    assertEquals(2f, c.getFloat("bar", 2f));
  }

  @Test
  void testGetInt() {
    var c = new Configuration<>();
    c.set("foo", "1");

    assertEquals(1, c.getInt("foo"));
  }

  @Test
  void testGetIntMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getInt("foo"));
  }

  @Test
  void testGetIntDefault() {
    var c = new Configuration<>();
    c.set("foo", "1");

    assertEquals(1, c.getInt("foo", 2));
    assertEquals(2, c.getInt("bar", 2));
  }

  @Test
  void testGetShort() {
    var c = new Configuration<>();
    c.set("foo", "1");

    assertEquals((short) 1, c.getShort("foo"));
  }

  @Test
  void testGetShortMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getShort("foo"));
  }

  @Test
  void testGetShortDefault() {
    var c = new Configuration<>();
    c.set("foo", "1");

    assertEquals((short) 1, c.getShort("foo", (short) 2));
    assertEquals((short) 2, c.getShort("bar", (short) 2));
  }

  @Test
  void testGetLongMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getLong("foo"));
  }

  @Test
  void testGetLong() {
    var c = new Configuration<>();
    c.set("foo", "1");

    assertEquals(1L, c.getLong("foo"));
  }

  @Test
  void testGetLongDefault() {
    var c = new Configuration<>();
    c.set("foo", "1");

    assertEquals(1L, c.getLong("foo", 2L));
    assertEquals(2L, c.getLong("bar", 2L));
  }

  @Test
  void testGetClass() throws ClassNotFoundException {
    var c = new Configuration<>();
    c.set("foo", Integer.class.getName());

    assertEquals(Integer.class, c.getClass("foo"));
  }

  @Test
  void testGetClassMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getClass("foo"));
  }

  @Test
  void testGetList() {
    var c = new Configuration<>();
    c.set("foo", "foo,bar");

    assertEquals(Arrays.asList("foo", "bar"), c.getList("foo"));
  }

  @Test
  void testGetListMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getList("foo"));
  }

  @Test
  void testGetListDefault() {
    var c = new Configuration<>();
    c.set("foo", "foo,bar");

    assertEquals(Arrays.asList("foo", "bar"), c.getList("foo", singletonList("that")));
    assertEquals(singletonList("that"), c.getList("bar", singletonList("that")));
  }

  @Test
  void testListener() {
    var c = new Configuration<Integer,String>();
    OnStoreCompacted<Integer,String> listener = (lastEntry, storeFile) -> System.out.println("Compacted");
    c.registerOnStoreCompactedListener(listener);

    OnStoreCompacted<?, ?> actual = c.getStoreCompactedEventListeners().get(0);
    assertEquals(listener, actual);
  }

  @Test
  void testConstructorWithSerializers() {
    var c = new Configuration<>(new TestStoreReader.PointSerializer(), new TestStoreReader.GenericAvroSerializer());
    assertEquals(TestStoreReader.PointSerializer.class, c.getSerializers().keySerializer().getClass());
    assertEquals(TestStoreReader.GenericAvroSerializer.class, c.getSerializers().valueSerializer().getClass());
  }
}
