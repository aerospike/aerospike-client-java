package com.aerospike.submillipost.exception;

public class InvalidUserHandleException extends SubMilliPostException {

    public InvalidUserHandleException(String handle) {
        super("validation_error", "Invalid handle format: " + handle + ". Must match [A-Za-z0-9_-]+");
    }
}
