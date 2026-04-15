package com.aerospike.submillipost.exception;

import com.aerospike.submillipost.controller.Headers;
import com.aerospike.submillipost.dto.response.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final String INTERNAL_ERROR = "internal_error";

    @ExceptionHandler(SubMilliPostException.class)
    public ResponseEntity<ErrorResponse> handleSubMilliPostException(SubMilliPostException e) {
        var status = mapErrorCodeToStatus(e.getErrorCode());
        return ResponseEntity.status(status).body(ErrorResponse.of(e.getErrorCode(), e.getMessage()));
    }

    /**
     * When a mandatory header is missing, treat a missing caller-handle header as
     * an auth failure (401). Any other missing header is a client bad request (400).
     */
    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ErrorResponse> handleMissingRequestHeader(MissingRequestHeaderException e) {
        if (Headers.CALLER_HANDLE.equalsIgnoreCase(e.getHeaderName())) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(ErrorResponse.of("unauthorized", "Authentication required"));
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.of("validation_error", "Missing required header: " + e.getHeaderName()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception e) {
        log.error("Unexpected error", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ErrorResponse.of(INTERNAL_ERROR, "An unexpected error occurred"));
    }

    private HttpStatus mapErrorCodeToStatus(String errorCode) {
        return switch (errorCode) {
            case "validation_error" -> HttpStatus.BAD_REQUEST;
            case "unauthorized" -> HttpStatus.UNAUTHORIZED;
            case "forbidden" -> HttpStatus.FORBIDDEN;
            case "not_found", "unavailable" -> HttpStatus.NOT_FOUND;
            case "conflict" -> HttpStatus.CONFLICT;
            default -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }
}
