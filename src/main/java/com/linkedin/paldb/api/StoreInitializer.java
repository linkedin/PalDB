package com.linkedin.paldb.api;

/**
 * Interface for initializing StoreRW with initial entries.
 * @param <K> key type
 * @param <V> value type
 */
public interface StoreInitializer<K,V> extends AutoCloseable {

    void put(K key, V value);

    void remove(K key);

    @Override
    void close();
}
