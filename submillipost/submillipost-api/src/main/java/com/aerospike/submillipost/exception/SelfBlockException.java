package com.aerospike.submillipost.exception;

public class SelfBlockException extends SubMilliPostException {

    public SelfBlockException() {
        super("validation_error", "Cannot block yourself");
    }
}
