package com.aerospike.submillipost.controller;

import com.aerospike.submillipost.AerospikeTestBase;
import com.aerospike.submillipost.dto.request.CreateUserRequest;
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
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class BlocksControllerIntegrationTest extends AerospikeTestBase {

    @Autowired
    private TestRestTemplate restTemplate;

    private static final String USERS_URL = "/api/v1/users";
    private static final String BLOCKS_URL_TMPL = "/api/v1/users/%s/blocks";
    private static final String BLOCKS_TARGET_URL_TMPL = "/api/v1/users/%s/blocks/%s";

    private static void assertStatus(HttpStatus expected, ResponseEntity<?> response) {
        assertEquals(expected.value(), response.getStatusCode().value(),
                () -> "expected " + expected + " but was " + response.getStatusCode()
                        + ", body=" + response.getBody());
    }

    @Test
    void block_returns201_andListReturnsTarget() {
        createUser("alice");
        createUser("bob");

        var blockResp = restTemplate.exchange(
                String.format(BLOCKS_TARGET_URL_TMPL, "alice", "bob"),
                HttpMethod.POST,
                new HttpEntity<>(headersFor("alice")), Void.class);
        assertStatus(HttpStatus.CREATED, blockResp);

        var listResp = restTemplate.exchange(
                String.format(BLOCKS_URL_TMPL, "alice"),
                HttpMethod.GET,
                new HttpEntity<>(headersFor("alice")), Map.class);
        assertStatus(HttpStatus.OK, listResp);
        var body = listResp.getBody();
        assertNotNull(body);
        assertEquals(List.of("bob"), body.get("items"));
        assertTrue(body.containsKey("next_cursor"));
        assertNull(body.get("next_cursor"));
    }

    @Test
    void block_self_returns400() {
        createUser("alice");

        var response = restTemplate.exchange(
                String.format(BLOCKS_TARGET_URL_TMPL, "alice", "alice"),
                HttpMethod.POST,
                new HttpEntity<>(headersFor("alice")), Map.class);

        assertStatus(HttpStatus.BAD_REQUEST, response);
        assertEquals("validation_error", getErrorCode(response.getBody()));
    }

    @Test
    void block_unknownTarget_returns404() {
        createUser("alice");

        var response = restTemplate.exchange(
                String.format(BLOCKS_TARGET_URL_TMPL, "alice", "ghost"),
                HttpMethod.POST,
                new HttpEntity<>(headersFor("alice")), Map.class);

        assertStatus(HttpStatus.NOT_FOUND, response);
        assertEquals("not_found", getErrorCode(response.getBody()));
    }

    @Test
    void block_wrongCaller_returns403() {
        createUser("alice");
        createUser("bob");

        var response = restTemplate.exchange(
                String.format(BLOCKS_TARGET_URL_TMPL, "alice", "bob"),
                HttpMethod.POST,
                new HttpEntity<>(headersFor("bob")), Map.class);

        assertStatus(HttpStatus.FORBIDDEN, response);
    }

    @Test
    void block_noAuth_returns401() {
        createUser("alice");
        createUser("bob");

        var response = restTemplate.exchange(
                String.format(BLOCKS_TARGET_URL_TMPL, "alice", "bob"),
                HttpMethod.POST,
                HttpEntity.EMPTY, Map.class);

        assertStatus(HttpStatus.UNAUTHORIZED, response);
    }

    @Test
    void unblock_returns204_andListBecomesEmpty() {
        createUser("alice");
        createUser("bob");
        // block first
        restTemplate.exchange(String.format(BLOCKS_TARGET_URL_TMPL, "alice", "bob"),
                HttpMethod.POST, new HttpEntity<>(headersFor("alice")), Void.class);

        var response = restTemplate.exchange(
                String.format(BLOCKS_TARGET_URL_TMPL, "alice", "bob"),
                HttpMethod.DELETE,
                new HttpEntity<>(headersFor("alice")), Void.class);

        assertStatus(HttpStatus.NO_CONTENT, response);

        var listResp = restTemplate.exchange(String.format(BLOCKS_URL_TMPL, "alice"),
                HttpMethod.GET, new HttpEntity<>(headersFor("alice")), Map.class);
        assertStatus(HttpStatus.OK, listResp);
        assertEquals(List.of(), listResp.getBody().get("items"));
    }

    @Test
    void listBlocked_emptyList_returns200() {
        createUser("alice");

        var response = restTemplate.exchange(
                String.format(BLOCKS_URL_TMPL, "alice"),
                HttpMethod.GET,
                new HttpEntity<>(headersFor("alice")), Map.class);

        assertStatus(HttpStatus.OK, response);
        assertEquals(List.of(), Objects.requireNonNull(response.getBody()).get("items"));
    }

    @Test
    void listBlocked_noAuth_returns401() {
        createUser("alice");

        var response = restTemplate.exchange(
                String.format(BLOCKS_URL_TMPL, "alice"),
                HttpMethod.GET,
                HttpEntity.EMPTY, Map.class);

        assertStatus(HttpStatus.UNAUTHORIZED, response);
    }

    @Test
    void listBlocked_wrongCaller_returns403() {
        createUser("alice");
        createUser("bob");

        var response = restTemplate.exchange(
                String.format(BLOCKS_URL_TMPL, "alice"),
                HttpMethod.GET,
                new HttpEntity<>(headersFor("bob")), Map.class);

        assertStatus(HttpStatus.FORBIDDEN, response);
    }

    // --- helpers ---

    private void createUser(String handle) {
        var resp = restTemplate.postForEntity(USERS_URL,
                new CreateUserRequest(handle, handle.substring(0, 1).toUpperCase() + handle.substring(1)),
                Map.class);
        assertStatus(HttpStatus.CREATED, resp);
    }

    private HttpHeaders headersFor(String callerHandle) {
        var h = new HttpHeaders();
        h.set(Headers.CALLER_HANDLE, callerHandle);
        return h;
    }

    @SuppressWarnings("unchecked")
    private String getErrorCode(Map<String, Object> body) {
        assertNotNull(body);
        var err =  (Map<String, Object>) body.get("error");
        return (String) err.get("code");
    }
}
