package com.linkedin.paldb.api;

import java.io.*;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface StoreRW<K,V> extends StoreReader<K,V>, Flushable {

    StoreInitializer<K,V> init();

    @SuppressWarnings("EmptyTryBlock")
    default void open() {
        try (var init = init()) {
            //nop
        }
    }

    void put(K key, V value);

    void remove(K key);

    @Override
    void flush();

    CompletableFuture<Map.Entry<K,V>> compact();
}
