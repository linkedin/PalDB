package com.linkedin.paldb.api;

public final class PalDBConfigBuilder {

    private final Configuration palDbConfiguration;

    private PalDBConfigBuilder() {
        this.palDbConfiguration = new Configuration();
    }

    private PalDBConfigBuilder(Configuration configuration) {
        this.palDbConfiguration = new Configuration(configuration, false);
    }

    public static PalDBConfigBuilder create() {
        return new PalDBConfigBuilder();
    }
    public static PalDBConfigBuilder create(Configuration fromConfig) {
        return new PalDBConfigBuilder(fromConfig);
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
     * <code>compression.enabled</code> - enable compression [default: false]
     * @param enabled flag
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withEnableCompression(boolean enabled) {
        palDbConfiguration.set(Configuration.COMPRESSION_ENABLED, String.valueOf(enabled));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>duplicates.enabled</code> - allow duplicates [default: false]
     * @param enabled flag
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withEnableDuplicates(boolean enabled) {
        palDbConfiguration.set(Configuration.ALLOW_DUPLICATES, String.valueOf(enabled));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>compression.enabled</code> - enable bloom filter [default: true]
     * @param enabled flag
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withEnableBloomFilter(boolean enabled) {
        palDbConfiguration.set(Configuration.BLOOM_FILTER_ENABLED, String.valueOf(enabled));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>compression.enabled</code> - bloom filter error rate [default: 0.01]
     * @param errorFactor value, e.g. 0.01 equals 1% error rate
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withBloomFilterErrorFactor(double errorFactor) {
        palDbConfiguration.set(Configuration.BLOOM_FILTER_ERROR_FACTOR, String.valueOf(errorFactor));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>write.buffer.size</code> - Write cache lements size, [default: 100_000]
     * @param elements value, e.g. 100_000
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder withWriteBufferElements(int elements) {
        palDbConfiguration.set(Configuration.WRITE_BUFFER_SIZE, String.valueOf(elements));
        return this;
    }

    public Configuration build() {
        return new Configuration(palDbConfiguration);
    }
}
