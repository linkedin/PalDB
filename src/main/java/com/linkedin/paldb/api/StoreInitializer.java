package com.linkedin.paldb.api;

public interface StoreInitializer<K,V> extends AutoCloseable {

    void put(K key, V value);

    @Override
    void close();
}
