package com.linkedin.paldb.impl;

import com.linkedin.paldb.api.*;
import com.linkedin.paldb.api.errors.StoreClosed;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class StoreRWImplTest {

    @Test
    void should_test_basic_operations(@TempDir Path tempDir) throws IOException, InterruptedException {
        var file = tempDir.resolve("test.paldb");

        var countDownLatch = new CountDownLatch(1);

        try (var sut = PalDB.createRW(file.toFile(),
                PalDBConfigBuilder.<String,String>create()
                        .withWriteBufferElements(2)
                        .withOnCompactedListener((lastEntry, storeFile) -> countDownLatch.countDown())
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

            countDownLatch.await(10, TimeUnit.SECONDS);
            assertTrue(Files.size(file) > size);

            assertNull(sut.get("non-existing-key"));

            sut.remove("any");
            assertNull(sut.get("any"));


            try (var stream = sut.stream()) {
                stream.forEach(System.out::println);
            }

            try (var stream = sut.stream()) {
                assertEquals(3, stream.count());
            }

            try (var keys = sut.streamKeys()) {
                assertEquals(3, keys.count());
            }
        }
    }

    @Test
    void should_put_and_remove_on_init(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.createRW(file.toFile(), PalDBConfigBuilder.<String,String>create().build())) {

            try (var init = sut.init()) {
                init.remove("any");
                init.put("any", "value");
                init.put("other", "value2");
                init.put("other", "value3");
                init.remove("other");
            }

            assertEquals(1, sut.size());
            sut.put("new", "element");
            assertEquals(2, sut.size());

            assertEquals("value", sut.get("any"));
            assertEquals("element", sut.get("new"));
            assertNull(sut.get("other"));

            sut.remove("any");
            assertEquals(2, sut.size()); //approximate count before flush
            sut.flush();
            assertEquals(1, sut.size());
            assertNull(sut.get("any"));
        }
    }

    @Test
    void should_clear_from_buffer_removed_after_flush(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.createRW(file.toFile(), PalDBConfigBuilder.<String,String>create().build())) {

            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
                init.put("other", "value3");
            }

            assertEquals(2, sut.size());
            sut.put("new", "element");
            assertEquals(3, sut.size());

            sut.remove("any");
            sut.remove("other");

            sut.flush();

            assertEquals(1, sut.size());
            assertNull(sut.get("any"));
            assertNull(sut.get("other"));
            assertEquals("element", sut.get("new"));
        }
    }

    @Test
    void should_put_trigger_compaction_in_background(@TempDir Path tempDir) throws IOException, InterruptedException {
        var file = tempDir.resolve("test.paldb");
        var countDownLatch = new CountDownLatch(1);

        try (var sut = PalDB.createRW(file.toFile(),
                PalDBConfigBuilder.<String,String>create()
                        .withWriteBufferElements(1)
                        .withOnCompactedListener((lastEntry, storeFile) -> countDownLatch.countDown())
                        .build())) {

            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }
            var size = Files.size(file);
            sut.put("new", "element");

            countDownLatch.await(10, TimeUnit.SECONDS);

            assertTrue(Files.size(file) > size);
        }
    }

    @Test
    void should_not_auto_flush_when_disabled(@TempDir Path tempDir) throws IOException {
        var file = tempDir.resolve("test.paldb");

        try (var sut = new StoreRWImpl<>(PalDBConfigBuilder.<String,String>create()
                        .withWriteBufferElements(1)
                        .withEnableWriteAutoFlush(false)
                        .build(),
                file.toFile())) {

            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }
            var size = Files.size(file);
            sut.put("new", "element");
            assertNull(sut.compactionFuture());
            assertEquals(size, Files.size(file));
        }
    }

    @Test
    void should_remove_trigger_compaction_in_background(@TempDir Path tempDir) throws IOException, InterruptedException {
        var file = tempDir.resolve("test.paldb");
        var countDownLatch = new CountDownLatch(1);
        try (var sut = PalDB.createRW(file.toFile(),
                PalDBConfigBuilder.<String,String>create()
                        .withWriteBufferElements(1)
                        .withOnCompactedListener((lastEntry, storeFile) -> countDownLatch.countDown())
                        .build())) {

            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }
            var size = Files.size(file);
            sut.remove("any");

            countDownLatch.await(10, TimeUnit.SECONDS);

            assertTrue(Files.size(file) < size);
        }
    }

    @Test
    void should_continue_working_even_when_compaction_listener_throws(@TempDir Path tempDir) throws IOException {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.createRW(file.toFile(),
                PalDBConfigBuilder.<String,String>create()
                        .withOnCompactedListener((lastEntry, storeFile) -> {
                            throw new RuntimeException("Error");
                        })
                        .build())) {

            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }
            var size = Files.size(file);
            sut.remove("any");
            sut.flush();

            assertTrue(Files.size(file) < size);

            size = Files.size(file);
            sut.put("new", "demoValue");
            assertEquals("demoValue", sut.get("new"));

            var lastEntry = sut.flushAsync().join();
            assertTrue(Files.size(file) > size);
            assertEquals("new", lastEntry.getKey());
            assertEquals("demoValue", lastEntry.getValue());
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
            assertThrows(StoreClosed.class, () -> {
                try (var stream = sut.stream()) {
                    stream.forEach(System.out::println);
                }
            });
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
    void should_trigger_compaction(@TempDir Path tempDir) throws IOException {
        var file = tempDir.resolve("test.paldb");
        try (var sut = PalDB.<String,String>createRW(file.toFile())) {
            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }
            var sizeBefore = Files.size(file);
            sut.put("foo", "bar");
            var lastEntry = sut.flushAsync().join();
            assertTrue(Files.size(file) > sizeBefore);
            assertEquals("foo", lastEntry.getKey());
            assertEquals("bar", lastEntry.getValue());
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
            sut.open();
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
        var configuration = PalDBConfigBuilder.<String,String>create()
                .withEnableDuplicates(true) //rw sets this to true if not already set
                .build();
        try (var sut = PalDB.createRW(file.toFile(), configuration)) {
            assertEquals(configuration, sut.getConfiguration());
        }
    }

    @Test
    void should_load_old_data_file(@TempDir Path tempDir) throws IOException {
        var file = tempDir.resolve("test.paldb");
        var configuration = PalDBConfigBuilder.<String,String>create()
                .withEnableDuplicates(true) //rw sets this to true if not already set
                .build();
        try (var sut = PalDB.createRW(file.toFile(), configuration)) {
            try (var init = sut.init()) {
                init.put("first", "value");
                init.put("second", "value2");
            }
        }
        assertTrue(Files.exists(file));
        assertTrue(Files.size(file) > 0);

        try (var sut = PalDB.createRW(file.toFile(), configuration)) {
            try (var init = sut.init()) {
                init.put("third", "value3");
                init.put("fourth", "value4");
            }
            assertEquals(4, sut.size());
            assertEquals("value", sut.get("first"));
            assertEquals("value2", sut.get("second"));
            assertEquals("value3", sut.get("third"));
            assertEquals("value4", sut.get("fourth"));
        }
    }

    @Test
    void should_invoke_on_compacted_listener_and_copy_store_file(@TempDir Path tempDir) throws IOException {
        var file = tempDir.resolve("test.paldb");
        var destStore = tempDir.resolve("any.paldb");
        var configuration = PalDBConfigBuilder.<String,String>create()
                .withOnCompactedListener((lastEntry, storeFile) -> {
                    try {
                        assertEquals("any", lastEntry.getKey());
                        assertEquals("new value", lastEntry.getValue());
                        var dest = storeFile.toPath().getParent().resolve(lastEntry.getKey() + ".paldb");
                        Files.copy(storeFile.toPath(), dest, StandardCopyOption.REPLACE_EXISTING);
                        System.out.println("Copied store to " + dest);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .build();
        try (var sut = PalDB.createRW(file.toFile(), configuration)) {
            try (var init = sut.init()) {
                init.put("first", "value");
                init.put("second", "value2");
            }

            assertFalse(Files.exists(destStore));

            sut.put("any", "new value");
            sut.flush();

            assertTrue(Files.exists(destStore));
            assertTrue(Files.size(destStore) > 0L);

            try (var destStoreRW = PalDB.createRW(destStore.toFile(), configuration)) {
                destStoreRW.open();

                assertEquals("new value", destStoreRW.get("any"));
                assertEquals("value", destStoreRW.get("first"));
                assertEquals("value2", destStoreRW.get("second"));
            }
        }
    }

    @Test
    void should_return_same_compaction_future_if_it_is_already_running(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        var configuration = PalDBConfigBuilder.<String,String>create()
                .withOnCompactedListener((lastEntry, storeFile) -> System.out.println("Compacted " + storeFile))
                .build();
        try (var sut = PalDB.createRW(file.toFile(), configuration)) {
            try (var init = sut.init()) {
                init.put("first", "value");
                init.put("second", "value2");
            }

            sut.put("one", "value one");
            sut.put("two", "value two");
            var future1 = sut.flushAsync();
            var future2 = sut.flushAsync();

            assertEquals(future1, future2);
            CompletableFuture.allOf(future1, future2).join();
        }
    }

    @Test
    void should_throw_when_file_is_null() {
        assertThrows(NullPointerException.class, () -> PalDB.createRW(null));
    }

    @Test
    void should_throw_when_config_is_null(@TempDir Path tempDir) {
        assertThrows(NullPointerException.class, () -> PalDB.createRW(tempDir.resolve("testStore.paldb").toFile(), null));
    }

    private static final AtomicInteger ix = new AtomicInteger(0);

    @Test
    @Tag("performance")
    //@RepeatedTest(5)
    void should_read_and_put_using_50_threads(@TempDir Path tempDir) throws InterruptedException {
        var file = tempDir.resolve("testMultiThread" + ix.getAndIncrement() + ".paldb");
        int threadCount = 50;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final AtomicBoolean success = new AtomicBoolean(true);
        var values = List.of("foobar", "any", "any value");
        var valuesAfterInit = List.of("foobar2", "any2", "any value 2");
        try (var store = PalDB.createRW(file.toFile(),
                PalDBConfigBuilder.<Integer,String>create()
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
    void should_not_return_same_key_when_iterating(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");

        try (var sut = new StoreRWImpl<>(PalDBConfigBuilder.<String,String>create()
                .withEnableWriteAutoFlush(false)
                .build(),
                file.toFile())) {

            try (var init = sut.init()) {
                init.put("any", "value");
                init.put("other", "value2");
            }

            sut.put("any", "updated value");

            try (var stream = sut.stream()) {
                var any = stream.filter(e -> e.getKey().equals("any"))
                        .collect(Collectors.toList());

                assertEquals(1, any.size());
                assertEquals("any", any.get(0).getKey());
                assertEquals("updated value", any.get(0).getValue());
            }

            sut.remove("other");
            try (var stream = sut.stream()) {
                var any = stream.filter(e -> e.getKey().equals("other"))
                        .collect(Collectors.toList());

                assertEquals(0, any.size());
            }
        }
    }

    @Test
    @Disabled
    @Tag("performance")
    void should_compact_35_million_keys_without_blocking(@TempDir Path tempDir) {
        var file = tempDir.resolve("test.paldb");
        try (var sut = new StoreRWImpl<>(
                PalDBConfigBuilder.<String, String>create()
                        .withWriteBufferElements(1)
                        .build(),
                file.toFile())) {

            int elements = 35_000_000;
            try (var init = sut.init()) {
                for (int i = 0; i < elements; i++) {
                    init.put(String.valueOf(i), "value " + i);
                }
            }

            var from = System.currentTimeMillis();

            sut.put("compact", "test"); //should compact

            assertEquals("test", sut.get("compact"));

            var elapsedMs = System.currentTimeMillis() - from;

            System.out.println(String.format("Compacted in %d ms", elapsedMs));

            assertTrue(elapsedMs < Duration.ofSeconds(5).toMillis());
            assertEquals(elements + 1L, sut.size());
        }
    }
}