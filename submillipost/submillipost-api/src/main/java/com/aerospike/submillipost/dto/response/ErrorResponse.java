package com.aerospike.submillipost.dto.response;

public record ErrorResponse(ErrorDetail error) {

    public record ErrorDetail(String code, String message) {}

    public static ErrorResponse of(String code, String message) {
        return new ErrorResponse(new ErrorDetail(code, message));
    }
}
