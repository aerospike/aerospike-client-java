package com.aerospike.submillipost.dto.response;

import com.aerospike.submillipost.model.User;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record UserResponse(
        String handle,
        @JsonProperty("display_name") String displayName,
        @JsonProperty("post_count") int postCount,
        @JsonProperty("note_count") int noteCount,
        @JsonProperty("follower_count") int followerCount,
        @JsonProperty("following_count") int followingCount,
        @JsonProperty("created_at_ms") long createdAtMs,
        String status,
        List<String> blocked
) {

    public static UserResponse from(User user) {
        return new UserResponse(
                user.handle(),
                user.displayName(),
                user.postCount(),
                user.noteCount(),
                user.followerCount(),
                user.followingCount(),
                user.createdAtMs(),
                user.status().value(),
                user.blocked()
        );
    }
}
