package com.linkedin.paldb.api;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class PalDBConfigBuilderTest {

    @Test
    public void testAllPropertiesSet() {
        var config = PalDBConfigBuilder.create()
                .withMemoryMapSegmentSize(500)
                .withMemoryMapDataEnabled(false)
                .withIndexLoadFactor(0.5)
                .withLRUCacheEnabled(true)
                .withCacheSizeLimit(200)
                .withCacheInitialCapacity(100)
                .withCacheLoadFactor(0.1)
                .withEnableCompression(true)
                .build();

        assertEquals(500, config.getInt(Configuration.MMAP_SEGMENT_SIZE));
        assertFalse(config.getBoolean(Configuration.MMAP_DATA_ENABLED));
        assertEquals(0.5, config.getDouble(Configuration.LOAD_FACTOR));
        assertTrue(config.getBoolean(Configuration.CACHE_ENABLED));
        assertEquals(200, config.getInt(Configuration.CACHE_BYTES));
        assertEquals(100, config.getInt(Configuration.CACHE_INITIAL_CAPACITY));
        assertEquals(0.1, config.getDouble(Configuration.CACHE_LOAD_FACTOR));
        assertTrue(config.getBoolean(Configuration.COMPRESSION_ENABLED));
    }
}