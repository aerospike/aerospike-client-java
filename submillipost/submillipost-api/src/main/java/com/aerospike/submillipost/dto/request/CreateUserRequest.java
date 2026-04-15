package com.aerospike.submillipost.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CreateUserRequest(
        String handle,
        @JsonProperty("display_name") String displayName
) {}
