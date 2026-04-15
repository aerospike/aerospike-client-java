package com.aerospike.submillipost.exception;

public class ForbiddenException extends SubMilliPostException {

    public ForbiddenException(String message) {
        super("forbidden", message);
    }
}
