package com.linkedin.paldb.impl;

import com.linkedin.paldb.api.*;
import com.linkedin.paldb.api.errors.StoreClosed;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.*;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class StoreRWImplTest {

    @Test
    void should_test_basic_operations(@TempDir Path tempDir) throws IOException {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile(),
                PalDBConfigBuilder.create()
                        .withWriteBufferElements(2)
                        .build())) {

            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }

            var size = Files.size(file);

            sut.put("third", "time");

            assertEquals("value", sut.get("any"));
            assertEquals("value2", sut.get("other"));
            assertEquals("time", sut.get("third"));

            assertEquals(size, Files.size(file));
            sut.put("fourth", "try"); //should compact
            assertEquals("time", sut.get("third"));
            assertEquals("try", sut.get("fourth"));

            assertTrue(Files.size(file) > size);

            assertNull(sut.get("non-existing-key"));

            sut.remove("any");
            assertNull(sut.get("any"));

            sut.forEach(System.out::println);

            assertEquals(3, StreamSupport.stream(sut.spliterator(), false).count());
            assertEquals(3, StreamSupport.stream(sut.keys().spliterator(), false).count());
        }
    }

    @Test
    void should_put_trigger_compaction(@TempDir Path tempDir) throws IOException {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile(),
                PalDBConfigBuilder.create()
                        .withWriteBufferElements(1)
                        .build())) {

            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }
            var size = Files.size(file);
            sut.put("new", "element");
            assertTrue(Files.size(file) > size);
        }
    }

    @Test
    void should_remove_trigger_compaction(@TempDir Path tempDir) throws IOException {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile(),
                PalDBConfigBuilder.create()
                        .withWriteBufferElements(1)
                        .build())) {

            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }
            var size = Files.size(file);
            sut.remove("any");
            assertTrue(Files.size(file) < size);
        }
    }

    @Test
    void should_throw_when_put_without_init(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile())) {
            assertThrows(StoreClosed.class, () -> sut.put("any", "value"));
        }
    }

    @Test
    void should_throw_when_get_without_init(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile())) {
            assertThrows(StoreClosed.class, () -> sut.get("any"));
        }
    }

    @Test
    void should_throw_when_remove_without_init(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile())) {
            assertThrows(StoreClosed.class, () -> sut.remove("any"));
        }
    }

    @Test
    void should_throw_when_iterating_without_init(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile())) {
            assertThrows(StoreClosed.class, () -> sut.forEach(System.out::println));
        }
    }

    @Test
    void should_throw_when_sizing_without_init(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile())) {
            assertThrows(StoreClosed.class, sut::size);
        }
    }

    @Test
    void should_trigger_compaction_when_flushing(@TempDir Path tempDir) throws IOException {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile())) {
            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }
            var sizeBefore = Files.size(file);
            sut.put("foo", "bar");
            sut.flush();
            assertTrue(Files.size(file) > sizeBefore);
        }
    }

    @Test
    void should_return_size_when_added_5_entries(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile())) {
            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }
            sut.put("foo", "bar");
            assertEquals(3, sut.size());
        }
    }

    @Test
    void should_return_size_zero_when_no_entries_added(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile())) {
            try (var init = sut.init()) {
                //
            }
            assertEquals(0, sut.size());
        }
    }

    @Test
    void should_return_file(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile())) {
            assertEquals(file.toFile(), sut.getFile());
        }
    }

    @Test
    void should_return_config(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        var configuration = PalDBConfigBuilder.create()
                .withEnableDuplicates(true) //rw sets this to true if not already set
                .build();
        try (var sut = PalDB.<String,String>createRW(file.toFile(), configuration)) {
            assertEquals(configuration, sut.getConfiguration());
        }
    }

    private static final AtomicInteger ix = new AtomicInteger(0);

   // @Test
    @RepeatedTest(5)
    void should_read_and_put_using_50_threads(@TempDir Path tempDir) throws InterruptedException {
        var file = tempDir.resolve("testMultiThread" + ix.getAndIncrement() + ".paldb");
        int threadCount = 50;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final AtomicBoolean success = new AtomicBoolean(true);
        var values = List.of("foobar", "any", "any value");
        var valuesAfterInit = List.of("foobar2", "any2", "any value 2");
        try (var store = PalDB.<Integer,String>createRW(file.toFile(),
                PalDBConfigBuilder.create()
                        .withWriteBufferElements(15)
                        .build())) {

            try (var init = store.init()) {
                for (int i = 0; i < values.size(); i++) {
                    init.put(i, values.get(i));
                }
            }

            for (int i = 0; i < valuesAfterInit.size(); i++) {
                store.put(i + values.size(), valuesAfterInit.get(i));
            }

            var counter = new AtomicInteger(100);

            for(int i = 0; i < threadCount; i++) {
                new Thread(() -> {
                    try {
                        var id = counter.getAndIncrement();
                        var name = "random" + id;
                        store.put(id, name);
                        for(int c = 0; c < 100000; c++) {
                            if(!success.get())break;
                            assertEquals("foobar", store.get(0));
                            assertEquals("any", store.get(1));
                            assertEquals("any value", store.get(2));
                            assertEquals("foobar2", store.get(3));
                            assertEquals("any2", store.get(4));
                            assertEquals("any value 2", store.get(5));
                            assertEquals(name, store.get(id));
                        }
                    } catch (Throwable error){
                        error.printStackTrace();
                        success.set(false);
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }
            latch.await();
            assertTrue(success.get());
        }
    }


    @Test
    @Disabled
    void should_rebuild_35_million_keys(@TempDir Path tempDir) throws IOException {
        var file = tempDir.resolve("test.paldb");
        try (var sut = new StoreRWImpl<String, String>(
                PalDBConfigBuilder.create()
                        .withWriteBufferElements(1)
                        .build(),
                file.toFile())) {

            int elements = 35_000_000;
            try (var init = sut.init()) {
                for (int i = 0; i < elements; i++) {
                    init.put(String.valueOf(i), "value " + i);
                }
            }

            var size = Files.size(file);

            var from = System.currentTimeMillis();

            sut.put("compact", "test"); //should compact

            assertTrue(Files.size(file) > size);

            System.out.println(String.format("Compacted in %d ms", System.currentTimeMillis() - from));

            assertEquals(elements + 1L, sut.size());
        }

    }
}