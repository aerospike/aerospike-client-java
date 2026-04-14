package com.aerospike.submillipost.dto.response;

import com.fasterxml.jackson.annotation.JsonProperty;

public record HealthResponse(
        String status,
        @JsonProperty("aerospike_connected") boolean aerospikeConnected
) {}