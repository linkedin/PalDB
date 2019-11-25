package com.linkedin.paldb.api.errors;

public class OutOfDiskSpace extends PalDBException {

    public OutOfDiskSpace(String message) {
        super(message);
    }
}
