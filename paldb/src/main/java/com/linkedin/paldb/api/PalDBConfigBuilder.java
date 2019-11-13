package com.linkedin.paldb.api;

public final class PalDBConfigBuilder {

    private final Configuration palDbConfiguration;

    private PalDBConfigBuilder() {
        this.palDbConfiguration = new Configuration();
    }

    public static PalDBConfigBuilder create() {
        return new PalDBConfigBuilder();
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>mmap.segment.size</code> - memory map segment size [default: 1GB]
     * @param bytes size in bytes
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withMemoryMapSegmentSize(long bytes) {
        palDbConfiguration.set(Configuration.MMAP_SEGMENT_SIZE, String.valueOf(bytes));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>mmap.data.enabled</code> - enable memory mapping for data [default: true]
     * @param enabled flag
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withMemoryMapDataEnabled(boolean enabled) {
        palDbConfiguration.set(Configuration.MMAP_DATA_ENABLED, String.valueOf(enabled));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>load.factor</code> - index load factor [default: 0.75]
     * @param loadFactor load factor value
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withIndexLoadFactor(double loadFactor) {
        palDbConfiguration.set(Configuration.LOAD_FACTOR, String.valueOf(loadFactor));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>cache.enabled</code> - LRU cache enabled [default: false]
     * @param enabled flag
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withLRUCacheEnabled(boolean enabled) {
        palDbConfiguration.set(Configuration.CACHE_ENABLED, String.valueOf(enabled));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>cache.bytes</code> - cache limit [default: Xmx - 100MB]
     * @param bytes size in bytes
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withCacheSizeLimit(long bytes) {
        palDbConfiguration.set(Configuration.CACHE_BYTES, String.valueOf(bytes));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>cache.initial.capacity</code> - cache initial capacity [default: 1000]
     * @param size number of initial capacity
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withCacheInitialCapacity(int size) {
        palDbConfiguration.set(Configuration.CACHE_INITIAL_CAPACITY, String.valueOf(size));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>cache.load.factor</code> - cache load factor [default: 0.75]
     * @param loadFactor load factor value
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withCacheLoadFactor(double loadFactor) {
        palDbConfiguration.set(Configuration.CACHE_LOAD_FACTOR, String.valueOf(loadFactor));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>compression.enabled</code> - enable compression [default: false]
     * @param enabled flag
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withEnableCompression(boolean enabled) {
        palDbConfiguration.set(Configuration.COMPRESSION_ENABLED, String.valueOf(enabled));
        return this;
    }

    public Configuration build() {
        return new Configuration(palDbConfiguration);
    }
}
