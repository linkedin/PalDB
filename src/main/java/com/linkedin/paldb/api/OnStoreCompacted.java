package com.linkedin.paldb.api;

import java.io.File;
import java.util.Map;

public interface OnStoreCompacted<K,V> {

    void apply(Map.Entry<K,V> lastEntry, File storeFile);

}
