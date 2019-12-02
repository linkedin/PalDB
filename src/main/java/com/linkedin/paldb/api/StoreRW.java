package com.linkedin.paldb.api;

import java.io.*;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Main interface to read and write data from/to a PalDB store.
 * <p>
 * <code>PalDB.createRW()</code> method and then call the
 * <code>init()</code> or <code>open()</code> method to open the store.
 * Call the <code>close()</code> to liberate resources when done.
 */
public interface StoreRW<K,V> extends StoreReader<K,V>, Flushable {

    /**
     * Opens the database with the ability to initialize it with the new entries.
     * StoreInitializer should be closed after usage.
     * @return AutoCloseable interface containing method `put` to add new entries.
     */
    StoreInitializer<K,V> init();

    /**
     * Opens the database without adding new entries
     */
    default void open() {
        var init = init();
        init.close();
    }

    /**
     * Puts key and value
     * @param key key to be added
     * @param value value to be added
     */
    void put(K key, V value);

    /**
     * Removes key from the store
     * @param key key to be removed
     */
    void remove(K key);

    /**
     * Flushes database file to disk with the latest entries from in-memory write buffer
     */
    @Override
    void flush();

    /**
     * Flushes database file to disk with the latest entries from in-memory write buffer asynchronously.
     * @return success future with last added entry or success future with null (if there is nothing to be flushed)
     *  or failed future
     */
    CompletableFuture<Map.Entry<K,V>> flushAsync();
}
