package com.aerospike.submillipost.exception;

public class SubMilliPostException extends RuntimeException {

    private final String errorCode;

    public SubMilliPostException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
