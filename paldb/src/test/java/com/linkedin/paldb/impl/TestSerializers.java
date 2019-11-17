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
import org.testng.annotations.*;

import java.awt.*;
import java.io.*;

import static org.testng.Assert.*;


public class TestSerializers {

  private Serializers serializers;

  @BeforeMethod
  public void setUp() {
    serializers = new Serializers();
  }

  @Test
  public void testRegister() {
    ColorSerializer i = new ColorSerializer();
    serializers.registerSerializer(i);
    assertSame(serializers.getSerializer(Color.class), i);
  }

  @Test
  public void testRegisterTwice() {
    ColorSerializer i1 = new ColorSerializer();
    ColorSerializer i2 = new ColorSerializer();
    serializers.registerSerializer(i1);
    serializers.registerSerializer(i2);
    assertSame(serializers.getSerializer(Color.class), i1);
  }

  @Test
  public void testRegisterTwo() {
    ColorSerializer i = new ColorSerializer();
    PointSerializer f = new PointSerializer();
    serializers.registerSerializer(i);
    serializers.registerSerializer(f);
    assertSame(serializers.getSerializer(Color.class), i);
    assertSame(serializers.getSerializer(Point.class), f);
  }

  @Test
  public void testGetSerializer() {
    ColorSerializer i = new ColorSerializer();
    serializers.registerSerializer(i);
    assertNull(serializers.getSerializer(Point.class));
    assertNotNull(serializers.getSerializer(Color.class));
  }

  @Test
  public void testSerialize() {
    serializers.registerSerializer(new ColorSerializer());
    assertNotNull(serializers.getSerializer(Color.class));
  }

  @Test
  public void testInterfaceType() {
    SerializerWithInterface i = new SerializerWithInterface();
    serializers.registerSerializer(i);
    assertSame(serializers.getSerializer(AnInterface.class), i);
  }

  // HELPER

  public static class ColorSerializer implements Serializer<Color> {

    @Override
    public Color read(DataInput input) {
      return null;
    }

    @Override
    public Class<Color> serializedClass() {
      return Color.class;
    }

    @Override
    public void write(DataOutput output, Color input) {

    }

  }

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

  public interface AnInterface {

  }

  public static class AClass implements AnInterface {

  }

  public static class SerializerWithInterface implements Serializer<AnInterface> {

    @Override
    public AnInterface read(DataInput input) {
      return null;
    }

    @Override
    public Class<AnInterface> serializedClass() {
      return AnInterface.class;
    }

    @Override
    public void write(DataOutput output, AnInterface input) {

    }

  }
}
