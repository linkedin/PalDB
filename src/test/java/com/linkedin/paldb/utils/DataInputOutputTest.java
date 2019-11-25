package com.linkedin.paldb.utils;

import org.junit.jupiter.api.*;

import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DataInputOutputTest {

    @Test
    void should_write_and_read_long() {
        var sut = new DataInputOutput();
        long max = 10_000_000L;

        LongStream.range(0L, max)
                .forEach(sut::writeLong);

        sut.reset();

        LongStream.range(0L, max)
                .forEach(value -> assertEquals(value, sut.readLong()));
    }
}