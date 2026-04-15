package com.aerospike.submillipost.model;

public enum UserStatus {
    ACTIVE("active"),
    DELETING("deleting");

    private final String value;

    UserStatus(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static UserStatus fromValue(String value) {
        for (var status : values()) {
            if (status.value.equals(value)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown status: " + value);
    }
}
