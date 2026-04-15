package com.aerospike.submillipost.repository;

/**
 * Aerospike bin name constants, grouped by entity.
 */
public final class Bins {

    private Bins() {}

    public static final class User {
        public static final String DISPLAY_NAME = "display_name";
        public static final String POST_IDS = "post_ids";
        public static final String NOTE_IDS = "note_ids";
        public static final String POST_CNT = "post_cnt";
        public static final String NOTE_CNT = "note_cnt";
        public static final String FOLLOWING_CNT = "following_cnt";
        public static final String FOLLOWER_CNT = "follower_cnt";
        public static final String BLOCKED = "blocked";
        public static final String CREATED_AT_MS = "created_at_ms";
        public static final String STATUS = "status";

        private User() {}
    }
}
