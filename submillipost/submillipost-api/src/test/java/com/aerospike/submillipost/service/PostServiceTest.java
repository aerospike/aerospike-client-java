package com.aerospike.submillipost.service;

import com.aerospike.submillipost.exception.ForbiddenException;
import com.aerospike.submillipost.exception.InvalidPostRequestException;
import com.aerospike.submillipost.exception.PostNotFoundException;
import com.aerospike.submillipost.exception.UnavailableException;
import com.aerospike.submillipost.model.Post;
import com.aerospike.submillipost.model.PostStatus;
import com.aerospike.submillipost.model.User;
import com.aerospike.submillipost.model.UserStatus;
import com.aerospike.submillipost.repository.PostRepository;
import com.aerospike.submillipost.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PostServiceTest {

    @Mock
    private PostRepository postRepository;

    @Mock
    private UserRepository userRepository;

    @Mock
    private UserBlockService blockService;

    @InjectMocks
    private PostService postService;

    private User aliceUser;
    private User bobUser;
    private Post activePost;

    @BeforeEach
    void setUp() {
        aliceUser = new User("alice", "Alice", 0, 0, 0, 0,
                1000L, UserStatus.ACTIVE, null, null, null);
        bobUser = new User("bob", "Bob", 0, 0, 0, 0,
                1000L, UserStatus.ACTIVE, null, null, null);
        activePost = new Post("p1", "T", "S", List.of("alice"),
                1_700_000_000_000L, "Body", 0, 0, 2000L, PostStatus.ACTIVE);
    }

    // --- createPost ---

    @Test
    void createPost_success_singleAuthor() {
        when(userRepository.findByHandle("alice")).thenReturn(aliceUser);
        when(postRepository.create(anyString(), eq("T"), eq("S"),
                eq(List.of("alice")), eq(1_700_000_000_000L), eq("Body")))
                .thenReturn(activePost);

        var result = postService.createPost("T", "S", List.of("alice"), 1_700_000_000_000L, "Body");

        assertNotNull(result);
        assertEquals("alice", result.authors().get(0));
        verify(userRepository).addPostToAuthor(eq("alice"), anyString());
    }

    @Test
    void createPost_cascadesToAllAuthors() {
        when(userRepository.findByHandle("alice")).thenReturn(aliceUser);
        when(userRepository.findByHandle("bob")).thenReturn(bobUser);
        when(postRepository.create(anyString(), anyString(), anyString(),
                any(), anyLong(), anyString()))
                .thenReturn(new Post("p2", "T", "S", List.of("alice", "bob"),
                        1L, "Body", 0, 0, 2000L, PostStatus.ACTIVE));

        postService.createPost("T", "S", List.of("alice", "bob"), 1L, "Body");

        verify(userRepository).addPostToAuthor(eq("alice"), anyString());
        verify(userRepository).addPostToAuthor(eq("bob"), anyString());
    }

    @Test
    void createPost_blankTitle_throwsValidation() {
        assertThrows(InvalidPostRequestException.class,
                () -> postService.createPost("", "S", List.of("alice"), 1L, "Body"));
        assertThrows(InvalidPostRequestException.class,
                () -> postService.createPost(null, "S", List.of("alice"), 1L, "Body"));
    }

    @Test
    void createPost_blankBody_throwsValidation() {
        assertThrows(InvalidPostRequestException.class,
                () -> postService.createPost("T", "S", List.of("alice"), 1L, ""));
        assertThrows(InvalidPostRequestException.class,
                () -> postService.createPost("T", "S", List.of("alice"), 1L, null));
    }

    @Test
    void createPost_emptyAuthors_throwsValidation() {
        assertThrows(InvalidPostRequestException.class,
                () -> postService.createPost("T", "S", List.of(), 1L, "Body"));
        assertThrows(InvalidPostRequestException.class,
                () -> postService.createPost("T", "S", null, 1L, "Body"));
    }

    @Test
    void createPost_unknownAuthor_throwsValidation() {
        when(userRepository.findByHandle("ghost")).thenReturn(null);

        assertThrows(InvalidPostRequestException.class,
                () -> postService.createPost("T", "S", List.of("ghost"), 1L, "Body"));
        verify(postRepository, never()).create(any(), any(), any(), any(), anyLong(), any());
    }

    // --- getPost ---

    @Test
    void getPost_success() {
        when(postRepository.findById("p1")).thenReturn(activePost);

        var result = postService.getPost("p1");

        assertEquals("p1", result.id());
    }

    @Test
    void getPost_notFound_throws() {
        when(postRepository.findById("unknown")).thenReturn(null);

        assertThrows(PostNotFoundException.class, () -> postService.getPost("unknown"));
    }

    @Test
    void getPost_deleting_throwsUnavailable() {
        var deletingPost = new Post("p1", "T", "S", List.of("alice"),
                1L, "Body", 0, 0, 2000L, PostStatus.MARKED_FOR_DELETION);
        when(postRepository.findById("p1")).thenReturn(deletingPost);

        assertThrows(UnavailableException.class, () -> postService.getPost("p1"));
    }

    // --- getPostForReader (inbound block enforcement) ---

    @Test
    void getPostForReader_noCaller_skipsBlockCheck() {
        when(postRepository.findById("p1")).thenReturn(activePost);

        var result = postService.getPostForReader("p1", null);

        assertEquals("p1", result.id());
        verifyNoInteractions(blockService);
    }

    @Test
    void getPostForReader_blankCaller_skipsBlockCheck() {
        when(postRepository.findById("p1")).thenReturn(activePost);

        var result = postService.getPostForReader("p1", "   ");

        assertEquals("p1", result.id());
        verifyNoInteractions(blockService);
    }

    @Test
    void getPostForReader_callerIsAuthor_skipsBlockCheckForSelf() {
        when(postRepository.findById("p1")).thenReturn(activePost);

        var result = postService.getPostForReader("p1", "alice");

        assertEquals("p1", result.id());
        // Author's own post — block check is not invoked for self.
        verify(blockService, never()).isBlockedBy("alice", "alice");
    }

    @Test
    void getPostForReader_authorBlockedCaller_throwsForbidden() {
        when(postRepository.findById("p1")).thenReturn(activePost);
        when(blockService.isBlockedBy("alice", "bob")).thenReturn(true);

        assertThrows(ForbiddenException.class,
                () -> postService.getPostForReader("p1", "bob"));
    }

    @Test
    void getPostForReader_noAuthorBlockedCaller_returnsPost() {
        when(postRepository.findById("p1")).thenReturn(activePost);
        when(blockService.isBlockedBy("alice", "bob")).thenReturn(false);

        var result = postService.getPostForReader("p1", "bob");

        assertEquals("p1", result.id());
    }

    @Test
    void getPostForReader_anyAuthorBlockedCaller_throwsForbidden() {
        var multiAuthorPost = new Post("p1", "T", "S", List.of("alice", "carol"),
                1L, "Body", 0, 0, 2000L, PostStatus.ACTIVE);
        when(postRepository.findById("p1")).thenReturn(multiAuthorPost);
        when(blockService.isBlockedBy("alice", "bob")).thenReturn(false);
        when(blockService.isBlockedBy("carol", "bob")).thenReturn(true);

        assertThrows(ForbiddenException.class,
                () -> postService.getPostForReader("p1", "bob"));
    }

    // --- updatePost ---

    @Test
    void updatePost_success() {
        when(postRepository.findById("p1")).thenReturn(activePost);
        var updated = new Post("p1", "T", "NewSub", List.of("alice"),
                1L, "NewBody", 0, 0, 2000L, PostStatus.ACTIVE);
        when(postRepository.updateSubtitleAndBody("p1", "NewSub", "NewBody")).thenReturn(updated);

        var result = postService.updatePost("p1", "NewSub", "NewBody");

        assertEquals("NewSub", result.subtitle());
        assertEquals("NewBody", result.body());
    }

    @Test
    void updatePost_notFound_throws() {
        when(postRepository.findById("unknown")).thenReturn(null);

        assertThrows(PostNotFoundException.class,
                () -> postService.updatePost("unknown", "S", "B"));
    }

    @Test
    void updatePost_deleting_throwsUnavailable() {
        var deletingPost = new Post("p1", "T", "S", List.of("alice"),
                1L, "Body", 0, 0, 2000L, PostStatus.MARKED_FOR_DELETION);
        when(postRepository.findById("p1")).thenReturn(deletingPost);

        assertThrows(UnavailableException.class,
                () -> postService.updatePost("p1", "S", "B"));
    }

    @Test
    void updatePost_blankFields_throwsValidation() {
        assertThrows(InvalidPostRequestException.class,
                () -> postService.updatePost("p1", "", "B"));
        assertThrows(InvalidPostRequestException.class,
                () -> postService.updatePost("p1", "S", ""));
    }

    // --- deletePost ---

    @Test
    void deletePost_success_cascadesAndDeletes() {
        var post = new Post("p1", "T", "S", List.of("alice", "bob"),
                1L, "Body", 0, 0, 2000L, PostStatus.ACTIVE);
        when(postRepository.findById("p1")).thenReturn(post);

        postService.deletePost("p1");

        InOrder order = inOrder(postRepository, userRepository);
        order.verify(postRepository).markForDelete("p1");
        order.verify(userRepository).removePostFromAuthor("alice", "p1");
        order.verify(userRepository).removePostFromAuthor("bob", "p1");
        order.verify(postRepository).delete("p1");
    }

    @Test
    void deletePost_notFound_throws() {
        when(postRepository.findById("unknown")).thenReturn(null);

        assertThrows(PostNotFoundException.class, () -> postService.deletePost("unknown"));
    }

    @Test
    void deletePost_alreadyDeleting_throwsUnavailable() {
        var deletingPost = new Post("p1", "T", "S", List.of("alice"),
                1L, "Body", 0, 0, 2000L, PostStatus.MARKED_FOR_DELETION);
        when(postRepository.findById("p1")).thenReturn(deletingPost);

        assertThrows(UnavailableException.class, () -> postService.deletePost("p1"));
    }

    // --- isAuthor ---

    @Test
    void isAuthor_returnsTrueWhenCallerInAuthors() {
        assertTrue(postService.isAuthor(activePost, "alice"));
    }

    @Test
    void isAuthor_returnsFalseForOthers() {
        assertFalse(postService.isAuthor(activePost, "bob"));
        assertFalse(postService.isAuthor(activePost, null));
    }
}
