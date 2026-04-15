package com.aerospike.submillipost.dto.response;

import com.aerospike.submillipost.model.Post;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record PostResponse(
        String id,
        String title,
        String subtitle,
        List<String> authors,
        @JsonProperty("pub_date_ms") long pubDateMs,
        String body,
        @JsonProperty("like_count") int likeCount,
        @JsonProperty("repost_count") int repostCount,
        @JsonProperty("created_at_ms") long createdAtMs,
        String status
) {

    public static PostResponse from(Post post) {
        return new PostResponse(
                post.id(),
                post.title(),
                post.subtitle(),
                post.authors(),
                post.pubDateMs(),
                post.body(),
                post.likeCount(),
                post.repostCount(),
                post.createdAtMs(),
                post.status().value()
        );
    }
}
