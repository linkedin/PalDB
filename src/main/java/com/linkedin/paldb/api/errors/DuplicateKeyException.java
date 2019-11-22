package com.linkedin.paldb.api.errors;

public class DuplicateKeyException extends PalDBException {

    public DuplicateKeyException(String message) {
        super(message);
    }
}
