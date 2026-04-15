package com.aerospike.submillipost.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CreatePostRequest(
        String title,
        String subtitle,
        List<String> authors,
        @JsonProperty("publish_date_ms") long publishDateMs,
        String body
) {}
