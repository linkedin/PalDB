package com.linkedin.paldb.api;

import com.linkedin.paldb.impl.TestSerializers;
import org.junit.jupiter.api.Test;

import java.awt.*;

import static org.junit.jupiter.api.Assertions.*;

class PalDBConfigBuilderTest {

    @Test
    void testAllPropertiesSet() {
        var config = PalDBConfigBuilder.<Color,Point>create()
                .withMemoryMapSegmentSize(500)
                .withMemoryMapDataEnabled(false)
                .withIndexLoadFactor(0.5)
                .withEnableCompression(true)
                .withEnableBloomFilter(true)
                .withBloomFilterErrorFactor(0.01)
                .withEnableDuplicates(true)
                .withWriteBufferElements(100)
                .withEnableWriteAutoFlush(false)
                .withOnCompactedListener((lastEntry, storeFile) -> System.out.println("works"))
                .withKeySerializer(new TestSerializers.ColorSerializer())
                .withValueSerializer(new TestSerializers.PointSerializer())
                .build();

        assertEquals(500, config.getInt(Configuration.MMAP_SEGMENT_SIZE));
        assertFalse(config.getBoolean(Configuration.MMAP_DATA_ENABLED));
        assertEquals(0.5, config.getDouble(Configuration.LOAD_FACTOR));
        assertTrue(config.getBoolean(Configuration.COMPRESSION_ENABLED));
        assertTrue(config.getBoolean(Configuration.BLOOM_FILTER_ENABLED));
        assertEquals(0.01, config.getDouble(Configuration.BLOOM_FILTER_ERROR_FACTOR));
        assertTrue(config.getBoolean(Configuration.ALLOW_DUPLICATES));
        assertEquals(100, config.getInt(Configuration.WRITE_BUFFER_SIZE));
        assertFalse(config.getBoolean(Configuration.WRITE_AUTO_FLUSH_ENABLED));
        assertNotNull(config.getStoreCompactedEventListeners().get(0));
        assertNotNull(config.getSerializers().getSerializer(Point.class));
        assertNotNull(config.getSerializers().getSerializer(Color.class));
    }
}