package com.linkedin.paldb.utils;

import org.junit.jupiter.api.*;

import java.io.IOException;
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

    @Test
    void should_skip_bytes() {
        var sut = new DataInputOutput(new byte[] {0,1,2,3,4});

        sut.skipBytes(4);
        assertEquals((byte)4, sut.readByte());
    }

    @Test
    void should_skip() {
        var sut = new DataInputOutput(new byte[] {0,1,2,3,4});

        sut.skip(4);
        assertEquals((byte)4, sut.readByte());
    }

    @Test
    void should_read_unsigned_short() {
        var sut = new DataInputOutput();
        sut.writeShort(50_000);
        sut.reset();
        assertEquals(50_000, sut.readUnsignedShort());
    }

    @Test
    void should_read_line() throws IOException {
        var sut = new DataInputOutput();
        sut.writeBytes("any value");
        sut.reset();
        assertEquals("any value", sut.readLine());
    }

    @Test
    void should_throw_when_read_object_write_object() {
        var sut = new DataInputOutput();
        assertThrows(UnsupportedOperationException.class, sut::readObject);
        assertThrows(UnsupportedOperationException.class, () -> sut.writeObject(null));
    }

    @Test
    void should_read() {
        var sut = new DataInputOutput();
        sut.writeByte(10);
        sut.reset();
        assertEquals(10, sut.read());
    }

    @Test
    void should_read_into_byte_array() {
        var sut = new DataInputOutput();
        var fooBytes = "foo".getBytes();
        sut.write(fooBytes);
        sut.reset();

        var bytes = new byte[fooBytes.length];
        assertEquals(fooBytes.length, sut.read(bytes));
        assertEquals("foo", new String(bytes));
    }

    @Test
    void should_read_into_byte_array_2() {
        var sut = new DataInputOutput();
        var fooBytes = "foo".getBytes();
        sut.write(fooBytes);
        sut.reset();

        var bytes = new byte[fooBytes.length];
        assertEquals(fooBytes.length, sut.read(bytes, 0, bytes.length));
        assertEquals("foo", new String(bytes));
    }

    @Test
    void should_flush() {
        var sut = new DataInputOutput();
        sut.flush();
        assertTrue(true);
    }
}