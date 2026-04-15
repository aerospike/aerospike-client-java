package com.aerospike.submillipost.dto.request;

public record UpdatePostRequest(
        String subtitle,
        String body
) {}
