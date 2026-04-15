package com.aerospike.submillipost.exception;

public class UserAlreadyExistsException extends SubMilliPostException {

    public UserAlreadyExistsException(String handle) {
        super("conflict", "User already exists: " + handle);
    }
}
