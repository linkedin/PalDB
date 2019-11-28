package com.linkedin.paldb.api.errors;

public class StoreIsCompacting extends PalDBException {
    public StoreIsCompacting(String message) {
        super(message);
    }
}
