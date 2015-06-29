PalDB
==========

PalDB is an embeddable write-once key-value store written in Java.

What is PalDB?
-------------------

PalDB is an embeddable persistent key-value store with very fast read performance and compact store size. PalDB stores are single binary files written once and ready to be used in applications.

PalDB's JAR is only 110K and has a single dependency (snappy, which isn't mandatory). It's also very easy to use with just a few configuration parameters.

Performance
-----------

Because PalDB is read-only it is significantly less complex than other embeddable key-value store and therefore benefits from optimization that wouldn't be possible if writes were permitted. PalDB is specifically optimized for read performance and store sizes. PalDB can be benchmarked compared to in-memory data structures such as Java collections (HashMap, HashSet) or other key-values stores (LevelDB, RocksDB). Best performance is achieved when memory mapping is enabled (default) and the entire store is loaded in off-heap memory.

Current benchmark on a 3Ghz Macbook Pro with 100M keys index shows an average performance of 2M reads/s for a memory usage 6X less than using a traditional HashSet.

Results of a throughput benchmark between PalDB, LevelDB and RocksDB (higher is better):

![throughput](http://linkedin.github.io/PalDB/doc/throughput.png)

Memory usage benchmark between PalDB and a Java HashSet (lower is better):

![memory](http://linkedin.github.com/PalDB/doc/memory.png)

What is it suitable for?
------------------------

PalDB can be used in applications which rely on side data prepared in advance and which only needs to be read. For example, machine learning applications such as machine translation, content classification or spam detection rely on large model files that are usually built and packaged by a separate process. These resources are often hard to deal with due to their size and the difficulty to load their content into memory when you need it.

PalDB can replace the usage of in-memory data structures to store static data with comparable read performance and by using an order of magnitude less memory. Because PalDB comes into a single file and requires no load time it's also very easy to distribute and use in applications.

Code samples
------------

API documentation can be found [here](http://linkedin.github.com/PalDB/doc/javadoc/index.html).

How to write a store
```java
StoreWriter writer = PalDB.createWriter(new File("store.paldb"));
writer.put("foo", "bar");
writer.put(1213, new int[] {1, 2, 3});
writer.close();
```

How to read a store
```java
StoreReader reader = PalDB.createReader(new File("store.paldb"));
String val1 = reader.get("foo");
int[] val2 = reader.get(1213);
reader.close();
```

How to iterate on a store
```java
StoreReader reader = PalDB.createReader(new File("store.paldb"));
Iterable<Map.Entry<String, String>> iterable = reader.iterable();
for (Map.Entry<String, String> entry : iterable) {
  String key = entry.getKey();
  String value = entry.getValue();      
}
reader.close();
```

Build
-----

PalDB requires Java 6 and gradle.

```bash
gradle build
```

Performance tests are run separately from the build
```bash
gradle perfTest
```

Test
----

We use the TestNG framework for our unit tests. You can run them via the `gradle clean test` command.

Advanced configuration
----------------------

Write parameters:

+ `load.factor`,  index load factor (double) [default: 0.75]
+ `compression.enabled`, enable compression (boolean) [default: false]

Read parameters:

+ `mmap.data.enabled`, enable memory mapping for data (boolean) [default: true]
+ `mmap.segment.size`, memory map segment size (bytes) [default: 1GB]
+ `cache.enabled`, LRU cache enabled (boolean) [default: false]
+ `cache.bytes`, cache limit (bytes) [default: Xmx - 100MB]
+ `cache.initial.capacity`, cache initial capacity (int) [default: 1000]
+ `cache.load.factor`, cache load factor (double) [default: 0.75]

Configuration values are passed at init time. Example:
```java
Configuration config = PalDB.newConfiguration();
config.set(Configuration.CACHE_ENABLED, "true");
StoreReader reader = PalDB.createReader(new File("store.paldb"), config);
```

A few tips on how configuration can affect performance:

+ Disabling memory mapping will significantly reduce performance as disk seeks will be performed instead.
+ Enabling the cache makes sense when the value size is large and there's a significant cost in deserialization. Otherwise, the cache adds an overhead. The cache is also useful when memory mapping is disabled.
+ Compression can be enabled when the store size is a concern and the values are large (e.g. a sparse matrix). By default, PalDB already uses a compact serialization. Snappy is used for compression.

Custom serializer
-----------------

PalDB is primarily optimized for Java primitives and arrays but supports adding custom serializers so arbitrary Java classes can be supported.

Serializers can be defined by implementing the `Serializer` interface and its methods. Here's an example which supports the `java.awt.Point` class:

```java
public class PointSerializer implements Serializer<Point> {

  @Override
  public Point read(DataInput input) {
    return new Point(input.readInt(), input.readInt());
  }

  @Override
  public void write(DataOutput output, Point point) {
    output.writeInt(point.x);
    output.writeInt(point.y);
  }

  @Override
  public int getWeight(Point instance) {
    return 8;
  }
}
```

The `write` method serializes the instance to the `DataOutput`. The `read` method deserializes from `DataInput` and creates new object instances. The `getWeight` method returns the estimated memory used by an instance in bytes. The latter is used by the cache to evaluate the amount of memory it's currently using.

Serializer implementation should be registered using the `Configuration`:

```java
Configuration configuration = PalDB.newConfiguration();
configuration.registerSerializer(new PointSerializer());
```

Use cases
---------

At LinkedIn, PalDB is used in data pipelines and machine-learning applications.

It's usage is popular on Hadoop work-flows because of its limited memory footprint and ease to distribute. It often allows to use map-side operations (e.g. join) when that wouldn't be possible with classic Java data structures. For instance, a set of 350M member ids would only use ~290M of memory with PalDB versus ~1.8GB with a traditional HashSet. Moreover, PalDB's store files are single binary files making it easy to package and use with Hadoop's distributed cache mechanism.

Machine-learning applications often have complex binary model files created in the `training` phase and used in the `scoring` phase. These two phases always happen at different times and often in different environments. For instance, the training phase happens on Hadoop or Spark and the scoring phase in a real-time service. PalDB makes this process easier and more efficient by reducing the need of large CSV files loaded in memory.

Limitations
-----------
+ PalDB is optimal in replacing the usage of large in-memory data storage but still use memory (off-heap, yet much less) to do its job. Disabling memory mapping and relying on seeks is possible but is not what PalDB has been optimized for.
+ The size of the index is limited to 2GB. There's no limitation in the data size however.
+ PalDB is not thread-safe at the moment so synchronization should be done externally if multi-threaded.

Contributions
-----------

Any helpful feedback is more than welcome. This includes feature requests, bug reports, pull requests, constructive feedback, etc.

Copyright & License
-------------------

PalDB © 2015 LinkedIn Corp. Licensed under the terms of the Apache License, Version 2.0.
