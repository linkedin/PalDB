package com.linkedin.paldb.api.errors;

public class MissingSerializer extends PalDBException {
    public MissingSerializer(String message) {
        super(message);
    }

    public MissingSerializer(Object obj) {
        super("The type '" + obj.getClass() + "' isn't supported");
    }
}
