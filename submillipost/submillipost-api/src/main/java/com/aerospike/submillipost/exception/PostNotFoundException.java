package com.aerospike.submillipost.exception;

public class PostNotFoundException extends SubMilliPostException {

    public PostNotFoundException(String postId) {
        super("not_found", "Post not found: " + postId);
    }
}
