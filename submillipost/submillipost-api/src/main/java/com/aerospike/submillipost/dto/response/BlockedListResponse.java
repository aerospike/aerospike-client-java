package com.aerospike.submillipost.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response shape for GET /api/v1/users/{handle}/blocks.
 * {@code next_cursor} is always null at L2 (pagination deferred) but included
 * in the shape so clients can be written forward-compatibly.
 */
public record BlockedListResponse(
        List<String> items,
        @JsonProperty("next_cursor")
        @JsonInclude(JsonInclude.Include.ALWAYS)
        String nextCursor
) {

    public static BlockedListResponse of(List<String> items) {
        return new BlockedListResponse(items, null);
    }
}
