package com.linkedin.paldb.api.errors;

public class OutOfDiskSpace extends RuntimeException {

    public OutOfDiskSpace(String message) {
        super(message);
    }
}
