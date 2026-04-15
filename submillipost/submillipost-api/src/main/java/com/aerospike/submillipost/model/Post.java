package com.aerospike.submillipost.model;

import java.util.List;

public record Post(
        String id,
        String title,
        String subtitle,
        List<String> authors,
        long pubDateMs,
        String body,
        int likeCount,
        int repostCount,
        long createdAtMs,
        PostStatus status
) {}
