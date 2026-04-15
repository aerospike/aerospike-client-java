package com.aerospike.submillipost.controller;

import com.aerospike.submillipost.dto.response.BlockedListResponse;
import com.aerospike.submillipost.exception.ForbiddenException;
import com.aerospike.submillipost.exception.UnauthorizedException;
import com.aerospike.submillipost.service.UserBlockService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/users/{handle}/blocks")
public class BlocksController {

    private final UserBlockService blockService;

    public BlocksController(@NonNull UserBlockService blockService) {
        this.blockService = blockService;
    }

    @GetMapping
    public ResponseEntity<BlockedListResponse> listBlocked(
            @PathVariable("handle") String handle,
            @RequestHeader(value = Headers.CALLER_HANDLE) String callerHandle) {
        requireAuth(callerHandle);
        requireSelf(handle, callerHandle, "Blocked list is private to the owner");
        return ResponseEntity.ok(BlockedListResponse.of(blockService.listBlocked(handle)));
    }

    @PostMapping("/{target}")
    public ResponseEntity<Void> block(
            @PathVariable("handle") String handle,
            @PathVariable("target") String target,
            @RequestHeader(value = Headers.CALLER_HANDLE) String callerHandle) {
        requireAuth(callerHandle);
        requireSelf(handle, callerHandle, "Cannot manage another user's blocks");
        blockService.block(handle, target);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @DeleteMapping("/{target}")
    public ResponseEntity<Void> unblock(
            @PathVariable("handle") String handle,
            @PathVariable("target") String target,
            @RequestHeader(value = Headers.CALLER_HANDLE) String callerHandle) {
        requireAuth(callerHandle);
        requireSelf(handle, callerHandle, "Cannot manage another user's blocks");
        blockService.unblock(handle, target);
        return ResponseEntity.noContent().build();
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
