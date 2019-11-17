package com.linkedin.paldb.utils;

import org.testng.annotations.*;

import java.lang.management.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.*;

import static org.testng.Assert.*;

public class BloomFilterTest {

    private int elements =  1_000_000;
    private int bitsize  = 10_000_000;
    private BloomFilter filter;
    private Random prng;
    private ThreadMXBean bean;

    @BeforeMethod
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
        sut1.add(HashUtils.hash("test".getBytes()));
        var sut1_1 = new BloomFilter(10_000, 0.01);
        sut1_1.add(HashUtils.hash("test".getBytes()));

        assertEquals(sut1.hashCode(), sut1_1.hashCode());
    }

    @Test
    void should_test_multi_thread_get() {
        var sut1 = new BloomFilter(10_000, 0.01);
        sut1.add(HashUtils.hash("test".getBytes()));

        var futures = IntStream.range(0, 1000)
                .mapToObj(i -> CompletableFuture.runAsync(() -> assertTrue(sut1.mightContain(HashUtils.hash("test".getBytes())))))
                .collect(Collectors.toList());

        futures.forEach(CompletableFuture::join);
    }

    @Test
    void correctness() {
        System.out.println("Testing correctness.\n"+
                "Creating a Set and filling it together with our filter...");
        filter.clear();
        Set<Long> inside = new HashSet<>((int)(elements / 0.75));
        while(inside.size() < elements) {
            long v = prng.nextLong();
            inside.add(v);
            filter.add(v);
            assertTrue(filter.mightContain(v), "There should be no false negative: " + v);
        }

        // testing
        int found = 0, total = 0;
        double rate = 0;
        while (total < elements) {
            long v = prng.nextLong();
            if (inside.contains(v)) continue;
            total++;
            found += filter.mightContain(v) ? 1 : 0;

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

        filter.clear();
        long start = bean.getCurrentThreadCpuTime();
        for(int i=0; i<elements; i++) filter.add(prng.nextInt());
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

        filter.clear();
        for(int i=0; i<elements; i++) filter.add(prng.nextLong());

        boolean xor = true; // Make sure our result isn’t optimized out
        long start = bean.getCurrentThreadCpuTime();
        for(int i=0; i<elements; i++) xor ^= filter.mightContain(prng.nextLong());
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