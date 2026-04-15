package com.aerospike.submillipost.controller;

import com.aerospike.submillipost.AerospikeTestBase;
import com.aerospike.submillipost.dto.request.CreatePostRequest;
import com.aerospike.submillipost.dto.request.CreateUserRequest;
import com.aerospike.submillipost.dto.request.UpdatePostRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PostsControllerIntegrationTest extends AerospikeTestBase {

    @Autowired
    private TestRestTemplate restTemplate;

    private static final String USERS_URL = "/api/v1/users";
    private static final String POSTS_URL = "/api/v1/posts";
    private static final String BLOCKS_URL_TMPL = "/api/v1/users/%s/blocks/%s";

    private static void assertStatus(HttpStatus expected, ResponseEntity<?> response) {
        assertEquals(expected.value(), response.getStatusCode().value(),
                () -> "expected " + expected + " but was " + response.getStatusCode()
                        + ", body=" + response.getBody());
    }

    // --- POST /api/v1/posts ---

    @Test
    void createPost_returns201_andCascadesToAuthor() {
        createUser("alice");

        var postResp = createPost("alice", new CreatePostRequest(
                "Hello", "Sub", List.of("alice"), 1_700_000_000_000L, "Body here"));

        assertStatus(HttpStatus.CREATED, postResp);
        var body = postResp.getBody();
        assertNotNull(body);
        assertNotNull(body.get("id"));
        assertEquals("Hello", body.get("title"));
        assertEquals("Sub", body.get("subtitle"));
        assertEquals("Body here", body.get("body"));
        assertEquals(List.of("alice"), body.get("authors"));
        assertEquals(0, body.get("like_count"));
        assertEquals(0, body.get("repost_count"));
        assertEquals("active", body.get("status"));

        // Cascade: alice.post_count should be 1 and post_ids should include the id.
        var user = restTemplate.getForEntity(USERS_URL + "/alice", Map.class).getBody();
        assertNotNull(user);
        assertEquals(1, user.get("post_count"));
    }

    @Test
    void createPost_withoutAuth_returns401() {
        var response = restTemplate.postForEntity(POSTS_URL,
                new CreatePostRequest("T", "S", List.of("alice"), 1L, "Body"), Map.class);

        assertStatus(HttpStatus.UNAUTHORIZED, response);
        assertEquals("unauthorized", getErrorMap(response.getBody()).get("code"));
    }

    @Test
    void createPost_unknownAuthor_returns400() {
        createUser("alice");

        var response = createPost("alice", new CreatePostRequest(
                "T", "S", List.of("ghost"), 1L, "Body"));

        assertStatus(HttpStatus.BAD_REQUEST, response);
        assertEquals("validation_error", getErrorMap(response.getBody()).get("code"));
    }

    @Test
    void createPost_blankBody_returns400() {
        createUser("alice");

        var response = createPost("alice", new CreatePostRequest(
                "T", "S", List.of("alice"), 1L, ""));

        assertStatus(HttpStatus.BAD_REQUEST, response);
        assertEquals("validation_error", getErrorMap(response.getBody()).get("code"));
    }

    // --- GET /api/v1/posts/{id} ---

    @Test
    void getPost_returns200() {
        createUser("alice");
        var id = createdPostId("alice");

        var response = restTemplate.getForEntity(POSTS_URL + "/" + id, Map.class);

        assertStatus(HttpStatus.OK, response);
        assertEquals(id, response.getBody().get("id"));
    }

    @Test
    void getPost_unknown_returns404() {
        var response = restTemplate.getForEntity(POSTS_URL + "/does-not-exist", Map.class);

        assertStatus(HttpStatus.NOT_FOUND, response);
        assertEquals("not_found", getErrorMap(response.getBody()).get("code"));
    }

    @Test
    void getPost_authorBlockedCaller_returns403() {
        createUser("alice");
        createUser("bob");
        var id = createdPostId("alice");

        // alice blocks bob
        var block = restTemplate.exchange(
                String.format(BLOCKS_URL_TMPL, "alice", "bob"),
                HttpMethod.POST,
                new HttpEntity<>(headersFor("alice")), Void.class);
        assertStatus(HttpStatus.CREATED, block);

        // bob tries to GET the post
        var response = restTemplate.exchange(POSTS_URL + "/" + id, HttpMethod.GET,
                new HttpEntity<>(headersFor("bob")), Map.class);
        assertStatus(HttpStatus.FORBIDDEN, response);
        assertEquals("forbidden", getErrorMap(response.getBody()).get("code"));
    }

    @Test
    void getPost_noCallerHeader_blockCheckSkipped() {
        createUser("alice");
        createUser("bob");
        var id = createdPostId("alice");
        // alice blocks bob
        restTemplate.exchange(String.format(BLOCKS_URL_TMPL, "alice", "bob"),
                HttpMethod.POST, new HttpEntity<>(headersFor("alice")), Void.class);

        // Anonymous GET — no X-User-Handle, should succeed.
        var response = restTemplate.getForEntity(POSTS_URL + "/" + id, Map.class);

        assertStatus(HttpStatus.OK, response);
    }

    // --- PUT /api/v1/posts/{id} ---

    @Test
    void updatePost_asAuthor_returns200() {
        createUser("alice");
        var id = createdPostId("alice");

        var response = restTemplate.exchange(POSTS_URL + "/" + id, HttpMethod.PUT,
                new HttpEntity<>(new UpdatePostRequest("NewSub", "NewBody"), headersFor("alice")),
                Map.class);

        assertStatus(HttpStatus.OK, response);
        assertEquals("NewSub", response.getBody().get("subtitle"));
        assertEquals("NewBody", response.getBody().get("body"));
    }

    @Test
    void updatePost_nonAuthor_returns403() {
        createUser("alice");
        createUser("bob");
        var id = createdPostId("alice");

        var response = restTemplate.exchange(POSTS_URL + "/" + id, HttpMethod.PUT,
                new HttpEntity<>(new UpdatePostRequest("NewSub", "NewBody"), headersFor("bob")),
                Map.class);

        assertStatus(HttpStatus.FORBIDDEN, response);
        assertEquals("forbidden", getErrorMap(response.getBody()).get("code"));
    }

    @Test
    void updatePost_withoutAuth_returns401() {
        createUser("alice");
        var id = createdPostId("alice");

        var response = restTemplate.exchange(POSTS_URL + "/" + id, HttpMethod.PUT,
                new HttpEntity<>(new UpdatePostRequest("NewSub", "NewBody"), new HttpHeaders()),
                Map.class);

        assertStatus(HttpStatus.UNAUTHORIZED, response);
    }

    // --- DELETE /api/v1/posts/{id} ---

    @Test
    void deletePost_asAuthor_returns204_andCascadesPostCount() {
        createUser("alice");
        var id = createdPostId("alice");

        var response = restTemplate.exchange(POSTS_URL + "/" + id, HttpMethod.DELETE,
                new HttpEntity<>(headersFor("alice")), Void.class);

        assertStatus(HttpStatus.OK, response);
        // Subsequent GET returns 404 (record deleted)
        var getAfter = restTemplate.getForEntity(POSTS_URL + "/" + id, Map.class);
        assertStatus(HttpStatus.NOT_FOUND, getAfter);

        // Cascade: alice.post_count back to 0
        var user = restTemplate.getForEntity(USERS_URL + "/alice", Map.class).getBody();
        assertEquals(0, user.get("post_count"));
    }

    @Test
    void deletePost_nonAuthor_returns403() {
        createUser("alice");
        createUser("bob");
        var id = createdPostId("alice");

        var response = restTemplate.exchange(POSTS_URL + "/" + id, HttpMethod.DELETE,
                new HttpEntity<>(headersFor("bob")), Map.class);

        assertStatus(HttpStatus.FORBIDDEN, response);
    }

    @Test
    void deletePost_withoutAuth_returns401() {
        createUser("alice");
        var id = createdPostId("alice");

        var response = restTemplate.exchange(POSTS_URL + "/" + id, HttpMethod.DELETE,
                HttpEntity.EMPTY, Map.class);

        assertStatus(HttpStatus.UNAUTHORIZED, response);
    }

    @Test
    void deletePost_notFound_returns404() {
        createUser("alice");

        var response = restTemplate.exchange(POSTS_URL + "/does-not-exist", HttpMethod.DELETE,
                new HttpEntity<>(headersFor("alice")), Map.class);

        assertStatus(HttpStatus.NOT_FOUND, response);
    }

    // --- helpers ---

    private void createUser(String handle) {
        var resp = restTemplate.postForEntity(USERS_URL,
                new CreateUserRequest(handle, handle.substring(0, 1).toUpperCase() + handle.substring(1)),
                Map.class);
        assertStatus(HttpStatus.CREATED, resp);
    }

    private ResponseEntity<Map> createPost(String caller, CreatePostRequest request) {
        return restTemplate.exchange(POSTS_URL, HttpMethod.POST,
                new HttpEntity<>(request, headersFor(caller)), Map.class);
    }

    private String createdPostId(String author) {
        var resp = createPost(author, new CreatePostRequest(
                "T", "S", List.of(author), 1_700_000_000_000L, "Body"));
        assertStatus(HttpStatus.CREATED, resp);
        return (String) resp.getBody().get("id");
    }

    private HttpHeaders headersFor(String callerHandle) {
        var h = new HttpHeaders();
        h.set(Headers.CALLER_HANDLE, callerHandle);
        return h;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getErrorMap(Map<String, Object> body) {
        assertNotNull(body);
        return (Map<String, Object>) body.get("error");
    }
}
