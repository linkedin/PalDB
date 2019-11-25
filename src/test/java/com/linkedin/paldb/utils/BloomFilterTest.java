package com.linkedin.paldb.utils;


import org.junit.jupiter.api.*;

import java.lang.management.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.*;


class BloomFilterTest {

    private int elements =  1_000_000;
    private int bitsize  = 10_000_000;
    private BloomFilter filter;
    private Random prng;
    private ThreadMXBean bean;

    @BeforeEach
    void setUp() {
        bean = ManagementFactory.getThreadMXBean();
        prng = new Random();
        prng.setSeed(0);
        filter = new BloomFilter(elements, bitsize);
    }

    @Test
    void should_calculate_bit_size_correctly() {
        var sut = new BloomFilter(10_000, 0.01);

        assertEquals(95851 , sut.bitSize());
    }

    @Test
    void should_calculate_hash_functions_correctly() {
        var sut = new BloomFilter(10_000, 0.01);

        assertEquals(7 , sut.hashFunctions());
    }

    @Test
    void should_provide_hashcode() {
        var sut1 = new BloomFilter(10_000, 0.01);
        sut1.add("test".getBytes());
        var sut1_1 = new BloomFilter(10_000, 0.01);
        sut1_1.add("test".getBytes());

        assertEquals(sut1.hashCode(), sut1_1.hashCode());
    }

    @Test
    void should_test_multi_thread_get() {
        var sut1 = new BloomFilter(10_000, 0.01);
        sut1.add("test".getBytes());

        var futures = IntStream.range(0, 1000)
                .mapToObj(i -> CompletableFuture.runAsync(() -> assertTrue(sut1.mightContain("test".getBytes()))))
                .collect(Collectors.toList());

        futures.forEach(CompletableFuture::join);
    }

    @Test
    void should_be_equal() {
        var bits = new long[Math.max(1, (int) Math.ceil((double) 6235225 / Long.SIZE))];
        var sut1 = new BloomFilter(4, 6235225, bits);
        var sut2 = new BloomFilter(4, 6235225, bits);

        for (int i = 0; i < 100; i++) {
            sut1.add((i + "foo").getBytes());
            sut2.add((i + "foo").getBytes());
        }

        assertEquals(sut1, sut2);
    }

    @Test
    void should_return_same_bits() {
        var bits = new long[Math.max(1, (int) Math.ceil((double) 6235225 / Long.SIZE))];
        var sut = new BloomFilter(4, 6235225, bits);

        assertArrayEquals(bits, sut.bits());
    }

    @Test
    void correctness() {
        System.out.println("Testing correctness.\n"+
                "Creating a Set and filling it together with our filter...");
        filter.clear();
        Set<Long> inside = new HashSet<>((int)(elements / 0.75));
        var bytes = new byte[10];
        while(inside.size() < elements) {
            var v = Math.abs(prng.nextLong());
            var pos = LongPacker.packLong(bytes, v);
            var valueBytes = Arrays.copyOf(bytes, pos);

            inside.add(v);
            filter.add(valueBytes);
            assertTrue(filter.mightContain(valueBytes), "There should be no false negative: " + v);
        }

        // testing
        int found = 0, total = 0;
        double rate = 0;
        while (total < elements) {
            var v = Math.abs(prng.nextLong());
            if (inside.contains(v)) continue;
            var pos = LongPacker.packLong(bytes, v);
            var valueBytes = Arrays.copyOf(bytes, pos);
            total++;
            found += filter.mightContain(valueBytes) ? 1 : 0;

            rate = (float) found / total;
            if (total % 1000 == 0 || total == elements) {
                System.out.format(
                        "\rElements incorrectly found to be inside: %8d/%-8d (%3.2f%%)",
                        found, total, 100*rate
                );
            }
        }
        System.out.println("\n");

        double ln2 = Math.log(2);
        double expectedRate = Math.exp(-ln2*ln2 * bitsize / elements);
        assertTrue(rate <= expectedRate * 1.10, "error rate p = e^(-ln2^2*m/n)");
    }

    @Test
    void insertion() {
        System.out.println("Testing insertion speed...");
        var bytes = new byte[10];
        filter.clear();
        long start = bean.getCurrentThreadCpuTime();
        for(int i=0; i<elements; i++) {
            var v = Math.abs(prng.nextLong());
            var pos = LongPacker.packLong(bytes, v);
            var valueBytes = Arrays.copyOf(bytes, pos);
            filter.add(valueBytes);
        }
        long end = bean.getCurrentThreadCpuTime();
        long time = end - start;

        System.out.format(
                "Inserted %d elements in %d ns.\n" +
                        "Insertion speed: %g elements/second\n\n",
                elements,
                time,
                elements/(time*1e-9)
        );
    }

    @Test
    void query() {
        System.out.println("Testing query speed...");
        var bytes = new byte[10];
        filter.clear();
        for(int i=0; i<elements; i++) {
            var v = Math.abs(prng.nextLong());
            var pos = LongPacker.packLong(bytes, v);
            var valueBytes = Arrays.copyOf(bytes, pos);
            filter.add(valueBytes);
        }

        boolean xor = true; // Make sure our result isn’t optimized out
        long start = bean.getCurrentThreadCpuTime();
        for(int i=0; i<elements; i++) {
            var v = Math.abs(prng.nextLong());
            var pos = LongPacker.packLong(bytes, v);
            var valueBytes = Arrays.copyOf(bytes, pos);
            xor ^= filter.mightContain(valueBytes);
        }
        long end = bean.getCurrentThreadCpuTime();
        long time = end - start;

        System.out.format(
                "Queried %d elements in %d ns.\n" +
                        "Query speed: %g elements/second\n\n",
                elements,
                time,
                elements/(time*1e-9)
        );
    }

}