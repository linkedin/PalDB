package com.linkedin.paldb.api;

public final class PalDBConfigBuilder<K,V> {

    private final Configuration<K,V> palDbConfiguration;

    private PalDBConfigBuilder() {
        this.palDbConfiguration = new Configuration<>();
    }

    private PalDBConfigBuilder(Configuration<K,V> configuration) {
        this.palDbConfiguration = new Configuration<>(configuration, false);
    }

    public static <K,V> PalDBConfigBuilder<K,V> create() {
        return new PalDBConfigBuilder<>();
    }
    public static <K,V> PalDBConfigBuilder<K,V> create(Configuration<K,V> fromConfig) {
        return new PalDBConfigBuilder<>(fromConfig);
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>mmap.segment.size</code> - memory map segment size [default: 1GB]
     * @param bytes size in bytes
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder<K,V> withMemoryMapSegmentSize(long bytes) {
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
    public PalDBConfigBuilder<K,V> withMemoryMapDataEnabled(boolean enabled) {
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
    public PalDBConfigBuilder<K,V> withIndexLoadFactor(double loadFactor) {
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
    public PalDBConfigBuilder<K,V> withEnableCompression(boolean enabled) {
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
    public PalDBConfigBuilder<K,V> withEnableDuplicates(boolean enabled) {
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
    public PalDBConfigBuilder<K,V> withEnableBloomFilter(boolean enabled) {
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
    public PalDBConfigBuilder<K,V> withBloomFilterErrorFactor(double errorFactor) {
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
    public PalDBConfigBuilder<K,V> withWriteBufferElements(int elements) {
        palDbConfiguration.set(Configuration.WRITE_BUFFER_SIZE, String.valueOf(elements));
        return this;
    }

    /**
     * <i>PalDB configuration property.</i>
     * <p>
     * <code>write.auto.flush.enabled</code> - enable writer auto flush [default: true]
     * Note that when true, auto flushing will be executing in the background. You can register
     * {@link #withOnCompactedListener} if you want to be notified after successful flush.
     * @param enabled flag
     * @return this {@code CachemeerConfigBuilder} instance (for chaining)
     */
    public PalDBConfigBuilder<K,V> withEnableWriteAutoFlush(boolean enabled) {
        palDbConfiguration.set(Configuration.WRITE_AUTO_FLUSH_ENABLED, String.valueOf(enabled));
        return this;
    }

    /**
     * Registers on store compacted listener
      * @param onCompactedListener listener
     * @return builder
     */
    public PalDBConfigBuilder<K,V> withOnCompactedListener(OnStoreCompacted<K,V> onCompactedListener) {
        palDbConfiguration.registerOnStoreCompactedListener(onCompactedListener);
        return this;
    }

    /**
     * Registers key serializer
     * @param serializer key serializer
     * @return builder
     */
    public PalDBConfigBuilder<K,V> withKeySerializer(Serializer<K> serializer) {
        palDbConfiguration.registerSerializer(serializer);
        return this;
    }

    /**
     * Registers value serializer
     * @param serializer value serializer
     * @return builder
     */
    public PalDBConfigBuilder<K,V> withValueSerializer(Serializer<V> serializer) {
        palDbConfiguration.registerSerializer(serializer);
        return this;
    }

    public Configuration<K,V> build() {
        return new Configuration<>(palDbConfiguration);
    }
}
