package com.ltv.aerospike.api.util;

public enum ErrorCode {
    SUCCESS(1000),
    LOGIN_FAILED(1001), //Login information is incorrect
    INPUT_DUPLICATE(1002), //This record already existed
    INPUT_REQUIRE(1003), //input data is required
    NOT_EXIST(1004), //This record doesn't exist
    ROLE_REQUIRE(1005), //role is required
    PERMISSION_REQUIRE(1006), //permission is required
    SAVE_FAILED(1007), //Save data failed
    DELETE_FAILED(1008); //Delete data failed

    private final Integer value;

    /**
     * @param value
     */
    ErrorCode(final Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
}
