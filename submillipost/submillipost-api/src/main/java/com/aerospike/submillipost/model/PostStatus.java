package com.aerospike.submillipost.model;

public enum PostStatus {
    ACTIVE("active"),
    MARKED_FOR_DELETION("marked_for_deletion");

    private final String value;

    PostStatus(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static PostStatus fromValue(String value) {
        for (var status : values()) {
            if (status.value.equals(value)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown status: " + value);
    }
}
