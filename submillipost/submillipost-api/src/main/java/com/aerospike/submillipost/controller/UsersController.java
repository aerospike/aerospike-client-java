package com.aerospike.submillipost.controller;

import com.aerospike.submillipost.dto.request.CreateUserRequest;
import com.aerospike.submillipost.dto.request.UpdateUserRequest;
import com.aerospike.submillipost.dto.response.UserResponse;
import com.aerospike.submillipost.exception.ForbiddenException;
import com.aerospike.submillipost.exception.UnauthorizedException;
import com.aerospike.submillipost.service.UserService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/users")
public class UsersController {

    private final UserService userService;

    public UsersController(@NonNull UserService userService) {
        this.userService = userService;
    }

    @PostMapping
    public ResponseEntity<UserResponse> createUser(@RequestBody CreateUserRequest request) {
        var user = userService.createUser(request.handle(), request.displayName());
        return ResponseEntity.status(HttpStatus.CREATED).body(UserResponse.from(user));
    }

    @GetMapping("/{handle}")
    public ResponseEntity<UserResponse> getUser(
            @PathVariable("handle") String handle,
            @RequestHeader(value = Headers.CALLER_HANDLE, required = false) String callerHandle) {
        var user = userService.getUser(handle, callerHandle);
        return ResponseEntity.ok(UserResponse.from(user));
    }

    @PutMapping("/{handle}")
    public ResponseEntity<UserResponse> updateUser(
            @PathVariable("handle") String handle,
            @RequestBody UpdateUserRequest request,
            @RequestHeader(value = Headers.CALLER_HANDLE, required = false) String callerHandle) {
        requireAuth(callerHandle);
        requireSelf(handle, callerHandle, "Profile update allowed for self only");
        var user = userService.updateUser(handle, request.displayName());
        return ResponseEntity.ok(UserResponse.from(user));
    }

    @DeleteMapping("/{handle}")
    public ResponseEntity<String> deleteUser(
            @PathVariable("handle") String handle,
            @RequestHeader(value = Headers.CALLER_HANDLE, required = false) String callerHandle) {
        requireAuth(callerHandle);
        requireSelf(handle, callerHandle, "Account deletion allowed for self only");
        userService.deleteUser(handle);
        return ResponseEntity.ok("User deleted");
    }

    private void requireAuth(String callerHandle) {
        if (callerHandle == null || callerHandle.isBlank()) {
            throw new UnauthorizedException();
        }
    }

    private void requireSelf(String handle, String callerHandle, String message) {
        if (!handle.equals(callerHandle)) {
            throw new ForbiddenException(message);
        }
    }
}
