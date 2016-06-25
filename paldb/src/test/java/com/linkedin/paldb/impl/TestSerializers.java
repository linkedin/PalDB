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
import com.linkedin.paldb.utils.DataInputOutput;

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestSerializers {

  private Serializers _serializers;

  @BeforeMethod
  public void setUp() {
    _serializers = new Serializers();
  }

  @Test
  public void testRegister() {
    ColorSerializer i = new ColorSerializer();
    _serializers.registerSerializer(i);
    Assert.assertSame(_serializers.getSerializer(Color.class), i);
    Assert.assertEquals(_serializers.getIndex(Color.class), 0);
  }

  @Test
  public void testRegisterTwice() {
    ColorSerializer i1 = new ColorSerializer();
    ColorSerializer i2 = new ColorSerializer();
    _serializers.registerSerializer(i1);
    _serializers.registerSerializer(i2);
    Assert.assertSame(_serializers.getSerializer(Color.class), i1);
  }

  @Test
  public void testRegisterTwo() {
    ColorSerializer i = new ColorSerializer();
    PointSerializer f = new PointSerializer();
    _serializers.registerSerializer(i);
    _serializers.registerSerializer(f);
    Assert.assertSame(_serializers.getSerializer(Color.class), i);
    Assert.assertEquals(_serializers.getIndex(Color.class), 0);
    Assert.assertSame(_serializers.getSerializer(Point.class), f);
    Assert.assertEquals(_serializers.getIndex(Point.class), 1);
  }

  @Test
  public void testGetSerializer() {
    ColorSerializer i = new ColorSerializer();
    _serializers.registerSerializer(i);
    Assert.assertNull(_serializers.getSerializer(Point.class));
    Assert.assertNotNull(_serializers.getSerializer(Color.class));
  }

  @Test
  public void testGetIndex() {
    ColorSerializer i = new ColorSerializer();
    _serializers.registerSerializer(i);
    Assert.assertEquals(_serializers.getIndex(Color.class), 0);
  }

  @Test
  public void testGetByIndex() {
    ColorSerializer i = new ColorSerializer();
    _serializers.registerSerializer(i);
    Assert.assertSame(_serializers.getSerializer(0), i);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetByIndexMissing() {
    _serializers.getSerializer(0);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMissingType() {
    MissingTypeSerializer i = new MissingTypeSerializer();
    _serializers.registerSerializer(i);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testObjectType() {
    ObjectTypeSerializer i = new ObjectTypeSerializer();
    _serializers.registerSerializer(i);
  }

  @Test
  public void testSerialize() throws Throwable {
    _serializers.registerSerializer(new ColorSerializer());
    DataInputOutput dio = new DataInputOutput();
    Serializers.serialize(dio, _serializers);
    byte[] bytes = dio.toByteArray();
    dio = new DataInputOutput(bytes);
    _serializers.clear();
    Serializers.deserialize(dio, _serializers);
    Assert.assertNotNull(_serializers.getSerializer(Color.class));
    Assert.assertEquals(_serializers.getIndex(Color.class), 0);
    Assert.assertNotNull(_serializers.getSerializer(0));
  }

  @Test
  public void testInterfaceType() throws Throwable {
    SerializerWithInterface i = new SerializerWithInterface();
    _serializers.registerSerializer(i);
    Assert.assertSame(_serializers.getSerializer(AnInterface.class), i);
  }

  // HELPER

  public static class ColorSerializer implements Serializer<Color> {

    @Override
    public Color read(DataInput input) {
      return null;
    }

    @Override
    public void write(DataOutput output, Color input) {

    }

    @Override
    public int getWeight(Color instance) {
      return 0;
    }
  }

  public static class PointSerializer implements Serializer<Point> {

    @Override
    public Point read(DataInput input) {
      return null;
    }

    @Override
    public void write(DataOutput output, Point input) {

    }

    @Override
    public int getWeight(Point instance) {
      return 0;
    }
  }

  public static class MissingTypeSerializer implements Serializer {

    @Override
    public Object read(DataInput input) {
      return null;
    }

    @Override
    public void write(DataOutput output, Object input) {

    }

    @Override
    public int getWeight(Object instance) {
      return 0;
    }
  }

  public static class ObjectTypeSerializer implements Serializer<Object> {

    @Override
    public Object read(DataInput input) {
      return null;
    }

    @Override
    public void write(DataOutput output, Object input) {

    }

    @Override
    public int getWeight(Object instance) {
      return 0;
    }
  }

  public static interface AnInterface {

  }

  public static class AClass implements AnInterface {

  }

  public static class SerializerWithInterface implements Serializer<AnInterface> {

    @Override
    public AnInterface read(DataInput input) {
      return null;
    }

    @Override
    public void write(DataOutput output, AnInterface input) {

    }

    @Override
    public int getWeight(AnInterface instance) {
      return 0;
    }
  }
}
