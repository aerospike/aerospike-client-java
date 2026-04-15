package com.aerospike.submillipost.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public record UpdateUserRequest(
        @JsonProperty("display_name") String displayName
) {}
