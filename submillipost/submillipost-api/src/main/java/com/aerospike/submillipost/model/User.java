package com.aerospike.submillipost.model;

import java.util.List;

public record User(
        String handle,
        String displayName,
        int postCount,
        int noteCount,
        int followerCount,
        int followingCount,
        long createdAtMs,
        UserStatus status,
        List<String> blocked,
        List<String> postIds,
        List<String> noteIds
) {}
