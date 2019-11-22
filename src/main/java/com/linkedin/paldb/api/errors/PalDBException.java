package com.linkedin.paldb.api.errors;

public abstract class PalDBException extends RuntimeException {

    public PalDBException(String message) {
        super(message);
    }
}
