package com.linkedin.paldb.api;

import java.io.*;

public interface StoreRW<K,V> extends StoreReader<K,V>, Flushable {

    StoreInitializer<K,V> init();

    void put(K key, V value);

    void remove(K key);

    @Override
    void flush();
}
