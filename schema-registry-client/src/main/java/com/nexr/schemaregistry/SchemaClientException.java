package com.nexr.schemaregistry;

public class SchemaClientException extends Exception{

    public SchemaClientException() {
        super();
    }

    public SchemaClientException(String message) {
        super(message);
    }

    public SchemaClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchemaClientException(Throwable cause) {
        super(cause);
    }
}
