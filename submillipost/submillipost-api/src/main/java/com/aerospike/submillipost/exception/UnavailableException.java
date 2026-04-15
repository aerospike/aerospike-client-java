package com.aerospike.submillipost.exception;

public class UnavailableException extends SubMilliPostException {

    public UnavailableException(String message) {
        super("unavailable", message);
    }
}
