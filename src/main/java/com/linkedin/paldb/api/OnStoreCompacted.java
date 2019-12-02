package com.linkedin.paldb.api;

import java.io.File;
import java.util.Map;

/**
 * Callback interface which #apply method is invoked, when store has been successfully flushed to disk.
 * @param <K> key type
 * @param <V> value type
 */
public interface OnStoreCompacted<K,V> {

    /**
     * Custom logic to apply when compaction has been finished.
     * Last entry put into the store before compaction and flushing to disk has been started.
     * Data or metadata from this entry could be used when copying storeFile to some backup location, e.g. lastEntry could have
     * kafka offset so it could be used when naming destination store file for accurate recovery.
     * It is guaranteed, that storeFile won't be changed until apply method has been completed.
     * @param lastEntry last added entry to the store before compaction
     * @param storeFile store file
     */
    void apply(Map.Entry<K,V> lastEntry, File storeFile);

}
