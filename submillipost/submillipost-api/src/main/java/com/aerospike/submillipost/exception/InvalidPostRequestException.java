package com.aerospike.submillipost.exception;

public class InvalidPostRequestException extends SubMilliPostException {

    public InvalidPostRequestException(String message) {
        super("validation_error", message);
    }
}
