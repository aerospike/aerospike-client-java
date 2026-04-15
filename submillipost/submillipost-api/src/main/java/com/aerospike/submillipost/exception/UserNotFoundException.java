package com.aerospike.submillipost.exception;

public class UserNotFoundException extends SubMilliPostException {

    public UserNotFoundException(String handle) {
        super("not_found", "User not found: " + handle);
    }
}
