package com.aerospike.submillipost.exception;

public class UnauthorizedException extends SubMilliPostException {

    public UnauthorizedException() {
        super("unauthorized", "X-User-Handle header is required");
    }
}
