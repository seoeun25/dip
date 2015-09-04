package com.nexr.dip;

public class DipLoaderException extends Exception{

    public DipLoaderException() {
        super();
    }

    public DipLoaderException(String message) {
        super(message);
    }

    public DipLoaderException(String message, Throwable cause) {
        super(message, cause);
    }

    public DipLoaderException(Throwable cause) {
        super(cause);
    }
}
