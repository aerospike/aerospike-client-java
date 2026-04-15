package com.aerospike.submillipost.controller;

import com.aerospike.submillipost.dto.request.CreatePostRequest;
import com.aerospike.submillipost.dto.request.UpdatePostRequest;
import com.aerospike.submillipost.dto.response.PostResponse;
import com.aerospike.submillipost.exception.ForbiddenException;
import com.aerospike.submillipost.exception.UnauthorizedException;
import com.aerospike.submillipost.model.Post;
import com.aerospike.submillipost.service.PostService;
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
@RequestMapping("/api/v1/posts")
public class PostsController {

    private final PostService postService;

    public PostsController(@NonNull PostService postService) {
        this.postService = postService;
    }

    @PostMapping
    public ResponseEntity<PostResponse> createPost(
            @RequestBody CreatePostRequest request,
            @RequestHeader(value = Headers.CALLER_HANDLE, required = false) String callerHandle) {
        requireAuth(callerHandle);
        var post = postService.createPost(
                request.title(), request.subtitle(), request.authors(),
                request.publishDateMs(), request.body());
        return ResponseEntity.status(HttpStatus.CREATED).body(PostResponse.from(post));
    }

    @GetMapping("/{id}")
    public ResponseEntity<PostResponse> getPost(
            @PathVariable("id") String postId,
            @RequestHeader(value = Headers.CALLER_HANDLE, required = false) String callerHandle) {
        var post = postService.getPostForReader(postId, callerHandle);
        return ResponseEntity.ok(PostResponse.from(post));
    }

    @PutMapping("/{id}")
    public ResponseEntity<PostResponse> updatePost(
            @PathVariable("id") String postId,
            @RequestBody UpdatePostRequest request,
            @RequestHeader(value = Headers.CALLER_HANDLE, required = false) String callerHandle) {
        requireAuth(callerHandle);
        var existing = postService.getPost(postId);
        requireAuthor(existing, callerHandle);
        var updated = postService.updatePost(postId, request.subtitle(), request.body());
        return ResponseEntity.ok(PostResponse.from(updated));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<String> deletePost(
            @PathVariable("id") String postId,
            @RequestHeader(value = Headers.CALLER_HANDLE, required = false) String callerHandle) {
        requireAuth(callerHandle);
        var existing = postService.getPost(postId);
        requireAuthor(existing, callerHandle);
        postService.deletePost(postId);
        return ResponseEntity.ok("Post deleted");

    }

    private void requireAuth(String callerHandle) {
        if (callerHandle == null || callerHandle.isBlank()) {
            throw new UnauthorizedException();
        }
    }

    private void requireAuthor(Post post, String callerHandle) {
        if (!postService.isAuthor(post, callerHandle)) {
            throw new ForbiddenException("Only an author can modify this post");
        }
    }
}
