package com.nexr.dip;

public class DipException extends Exception {

    public DipException() {
        super();
    }

    public DipException(String message) {
        super(message);
    }

    public DipException(String message, Throwable cause) {
        super(message, cause);
    }

    public DipException(Throwable cause) {
        super(cause);
    }

}