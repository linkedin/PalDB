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


import org.junit.jupiter.api.Test;

import java.awt.*;
import java.io.*;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;


public class TestConfiguration {

  @Test
  public void testConfiguration() {
    Configuration c = new Configuration();
    c.set("foo", "bar");
    assertEquals(c.get("foo", null), "bar");
    assertEquals(c.get("bar", "foo"), "foo");
  }

  @Test
  public void testConfigurationCopy() {
    Configuration c = new Configuration();
    c.set("foo", "bar");

    Configuration r = new Configuration(c);
    assertEquals(r.get("foo", null), "bar");

    c.set("foo", "");
    assertEquals(r.get("foo", null), "bar");
  }

  @Test
  public void testConfigurationReadOnly() {
    Configuration c = new Configuration();
    c.set("foo", "bar");

    assertThrows(UnsupportedOperationException.class, () -> {
      Configuration r = new Configuration(c);
      r.set("foo", "bar");
    });
  }

  @Test
  public void testEqualsEmpty() {
    assertEquals(new Configuration(), new Configuration());
  }

  @Test
  public void testEquals() {
    Configuration c1 = new Configuration();
    c1.set("foo", "bar");

    Configuration c2 = new Configuration();
    c2.set("foo", "bar");

    Configuration c3 = new Configuration();
    c3.set("foo", "notbar");

    assertEquals(c1, c2);
    assertNotEquals(c1, c3);
  }

  @Test
  public void testGetBoolean() {
    Configuration c = new Configuration();
    c.set("foo", "true");
    c.set("bar", "false");

    assertTrue(c.getBoolean("foo"));
    assertFalse(c.getBoolean("bar"));
  }

  @Test
  public void testGetBooleanMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getBoolean("foo"));
  }

  @Test
  public void testGetBooleanDefault() {
    Configuration c = new Configuration();
    c.set("foo", "true");

    assertTrue(c.getBoolean("foo", false));
    assertTrue(c.getBoolean("bar", true));
  }

  @Test
  public void testGetDouble() {
    Configuration c = new Configuration();
    c.set("foo", "1.0");

    assertEquals(c.getDouble("foo"), 1.0);
  }

  @Test
  public void testGetDoubleMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getDouble("foo"));
  }

  @Test
  public void testGetDoubleDefault() {
    Configuration c = new Configuration();
    c.set("foo", "1.0");

    assertEquals(c.getDouble("foo", 2.0), 1.0);
    assertEquals(c.getDouble("bar", 2.0), 2.0);
  }

  @Test
  public void testGetFloat() {
    Configuration c = new Configuration();
    c.set("foo", "1.0");

    assertEquals(c.getFloat("foo"), 1f);
  }

  @Test
  public void testGetFloatMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getFloat("foo"));
  }

  @Test
  public void testGetFloatDefault() {
    Configuration c = new Configuration();
    c.set("foo", "1.0");

    assertEquals(c.getFloat("foo", 2f), 1f);
    assertEquals(c.getFloat("bar", 2f), 2f);
  }

  @Test
  public void testGetInt() {
    Configuration c = new Configuration();
    c.set("foo", "1");

    assertEquals(c.getInt("foo"), 1);
  }

  @Test
  public void testGetIntMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getInt("foo"));
  }

  @Test
  public void testGetIntDefault() {
    Configuration c = new Configuration();
    c.set("foo", "1");

    assertEquals(c.getInt("foo", 2), 1);
    assertEquals(c.getInt("bar", 2), 2);
  }

  @Test
  public void testGetShort() {
    Configuration c = new Configuration();
    c.set("foo", "1");

    assertEquals(c.getShort("foo"), (short) 1);
  }

  @Test
  public void testGetShortMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getShort("foo"));
  }

  @Test
  public void testGetShortDefault() {
    Configuration c = new Configuration();
    c.set("foo", "1");

    assertEquals(c.getShort("foo", (short) 2), (short) 1);
    assertEquals(c.getShort("bar", (short) 2), (short) 2);
  }

  @Test
  public void testGetLongMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getLong("foo"));
  }

  @Test
  public void testGetLong() {
    Configuration c = new Configuration();
    c.set("foo", "1");

    assertEquals(c.getLong("foo"), 1L);
  }

  @Test
  public void testGetLongDefault() {
    Configuration c = new Configuration();
    c.set("foo", "1");

    assertEquals(c.getLong("foo", 2L), 1L);
    assertEquals(c.getLong("bar", 2L), 2L);
  }

  @Test
  public void testGetClass()
      throws ClassNotFoundException {
    Configuration c = new Configuration();
    c.set("foo", Integer.class.getName());

    assertEquals(c.getClass("foo"), Integer.class);
  }

  @Test
  public void testGetClassMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getClass("foo"));
  }

  @Test
  public void testGetList() {
    Configuration c = new Configuration();
    c.set("foo", "foo,bar");

    assertEquals(c.getList("foo"), Arrays.asList("foo", "bar"));
  }

  @Test
  public void testGetListMissing() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration().getList("foo"));
  }

  @Test
  public void testGetListDefault() {
    Configuration c = new Configuration();
    c.set("foo", "foo,bar");

    assertEquals(c.getList("foo", singletonList("that")), Arrays.asList("foo", "bar"));
    assertEquals(c.getList("bar", singletonList("that")), singletonList("that"));
  }

  // UTILITY

  public static class PointSerializer implements Serializer<Point> {

    @Override
    public Point read(DataInput input) {
      return null;
    }

    @Override
    public Class<Point> serializedClass() {
      return Point.class;
    }

    @Override
    public void write(DataOutput output, Point input) {

    }

  }
}
