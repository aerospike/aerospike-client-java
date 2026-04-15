package com.aerospike.submillipost.service;

import com.aerospike.submillipost.exception.ForbiddenException;
import com.aerospike.submillipost.exception.InvalidPostRequestException;
import com.aerospike.submillipost.exception.PostNotFoundException;
import com.aerospike.submillipost.exception.UnavailableException;
import com.aerospike.submillipost.model.Post;
import com.aerospike.submillipost.model.PostStatus;
import com.aerospike.submillipost.repository.PostRepository;
import com.aerospike.submillipost.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
public class PostService {

    private static final Logger log = LoggerFactory.getLogger(PostService.class);

    private final PostRepository postRepository;
    private final UserRepository userRepository;
    private final UserBlockService blockService;

    public PostService(PostRepository postRepository,
                       UserRepository userRepository,
                       UserBlockService blockService) {
        this.postRepository = postRepository;
        this.userRepository = userRepository;
        this.blockService = blockService;
    }

    public Post createPost(String title,
                           String subtitle,
                           List<String> authors,
                           long pubDateMs,
                           String body) {
        validateCreate(title, authors, body);
        for (var author : authors) {
            // TODO replace with findAllAuthors
            if (userRepository.findByHandle(author) == null) {
                throw new InvalidPostRequestException("Unknown author: " + author);
            }
        }

        var postId = UUID.randomUUID().toString();
        var post = postRepository.create(postId, title, subtitle, authors, pubDateMs, body);

        // append post_id and increment post_cnt for every author.
        for (var author : authors) {
            try {
                userRepository.addPostToAuthor(author, postId);
            } catch (RuntimeException e) {
                log.warn("Failed to cascade post {} to author {}: {}", postId, author, e.getMessage());
            }
        }
        return post;
    }

    /**
     * Raw fetch with status guard only. Used by internal/author-facing flows
     * (update/delete) where the caller is already known to be an author and
     * an inbound block check would be a no-op.
     */
    public Post getPost(String postId) {
        var post = postRepository.findById(postId);
        if (post == null) {
            throw new PostNotFoundException(postId);
        }
        if (post.status() == PostStatus.MARKED_FOR_DELETION) {
            throw new UnavailableException("Post is being deleted: " + postId);
        }
        return post;
    }

    /**
     * If any author has blocked the caller, responds with ForbiddenException.
     * For anonymous user, the block check is skipped.
     */
    public Post getPostForReader(String postId, String callerHandle) {
        var post = getPost(postId);
        if (callerHandle == null || callerHandle.isBlank() || post.authors() == null) {
            return post;
        }
        for (var author : post.authors()) {
            if (!author.equals(callerHandle) && blockService.isBlockedBy(author, callerHandle)) {
                throw new ForbiddenException("Blocked by content owner");
            }
        }
        for (var author : post.authors()) {
            if (!author.equals(callerHandle) && blockService.isBlockedBy(author, callerHandle)) {
                throw new ForbiddenException("Blocked by content owner");
            }
        }
        return post;
    }

    public Post updatePost(String postId, String subtitle, String body) {
        validateUpdate(subtitle, body);
        var existing = postRepository.findById(postId);
        if (existing == null) {
            throw new PostNotFoundException(postId);
        }
        if (existing.status() == PostStatus.MARKED_FOR_DELETION) {
            throw new UnavailableException("Post is being deleted: " + postId);
        }
        var updated = postRepository.updateSubtitleAndBody(postId, subtitle, body);
        if (updated == null) {
            throw new PostNotFoundException(postId);
        }
        return updated;
    }

    public void deletePost(String postId) {
        var post = postRepository.findById(postId);
        if (post == null) {
            throw new PostNotFoundException(postId);
        }
        if (post.status() == PostStatus.MARKED_FOR_DELETION) {
            throw new UnavailableException("Post is being deleted: " + postId);
        }

        postRepository.markForDelete(postId);

        for (var author : post.authors()) {
            try {
                userRepository.removePostFromAuthor(author, postId);
            } catch (RuntimeException e) {
                log.warn("Failed to cascade post delete {} from author {}: {}", postId, author, e.getMessage());
            }
        }

        // 3. Delete the post record.
        postRepository.delete(postId);
    }

    /** Returns true if callerHandle is in the post's authors list. */
    public boolean isAuthor(Post post, String callerHandle) {
        return callerHandle != null && post.authors() != null && post.authors().contains(callerHandle);
    }

    private void validateCreate(String title, List<String> authors, String body) {
        if (title == null || title.isBlank()) {
            throw new InvalidPostRequestException("Title is required");
        }
        if (body == null || body.isBlank()) {
            throw new InvalidPostRequestException("Body is required");
        }
        if (authors == null || authors.isEmpty()) {
            throw new InvalidPostRequestException("At least one author is required");
        }
    }

    private void validateUpdate(String subtitle, String body) {
        if (subtitle == null || subtitle.isBlank()) {
            throw new InvalidPostRequestException("Subtitle is required");
        }
        if (body == null || body.isBlank()) {
            throw new InvalidPostRequestException("Body is required");
        }
    }
}
