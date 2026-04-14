package com.aerospike.submillipost.controller;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.submillipost.dto.response.HealthResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {

    private final IAerospikeClient client;

    public HealthController(IAerospikeClient client) {
        this.client = client;
    }

    @GetMapping("/health")
    public HealthResponse health() {
        boolean connected = client.isConnected();
        String status = connected ? "ok" : "degraded";
        return new HealthResponse(status, connected);
    }
}
