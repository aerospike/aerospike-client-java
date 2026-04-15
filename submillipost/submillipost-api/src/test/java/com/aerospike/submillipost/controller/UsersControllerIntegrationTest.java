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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class UsersControllerIntegrationTest extends AerospikeTestBase {

    @Autowired
    private TestRestTemplate restTemplate;

    private static final String BASE_URL = "/api/v1/users";

    private static void assertStatus(HttpStatus expected, ResponseEntity<?> response) {
        assertEquals(expected.value(), response.getStatusCode().value(),
                () -> "expected " + expected + " but was " + response.getStatusCode() + ", body=" + response.getBody());
    }

    // --- POST /api/v1/users ---

    @Test
    void createUser_returns201() {
        var request = new CreateUserRequest("test-create", "Test Create");

        var response = restTemplate.postForEntity(BASE_URL, request, Map.class);

        assertStatus(HttpStatus.CREATED, response);
        var body = response.getBody();
        assertNotNull(body);
        assertEquals("test-create", body.get("handle"));
        assertEquals("Test Create", body.get("display_name"));
        assertEquals(0, body.get("post_count"));
        assertEquals(0, body.get("note_count"));
        assertEquals(0, body.get("follower_count"));
        assertEquals(0, body.get("following_count"));
        assertEquals("active", body.get("status"));
        assertNotNull(body.get("created_at_ms"));
    }

    @Test
    void createUser_invalidHandle_returns400() {
        var request = new CreateUserRequest("bad@handle", "Bad Handle");

        var response = restTemplate.postForEntity(BASE_URL, request, Map.class);

        assertStatus(HttpStatus.BAD_REQUEST, response);
        var error = getErrorMap(response.getBody());
        assertEquals("validation_error", error.get("code"));
    }

    @Test
    void createUser_duplicateHandle_returns409() {
        var request = new CreateUserRequest("test-dup", "Dup User");
        restTemplate.postForEntity(BASE_URL, request, Map.class);

        var response = restTemplate.postForEntity(BASE_URL, request, Map.class);

        assertStatus(HttpStatus.CONFLICT, response);
        var error = getErrorMap(response.getBody());
        assertEquals("conflict", error.get("code"));
    }

    // --- GET /api/v1/users/{handle} ---

    @Test
    void getUser_returns200() {
        var created = restTemplate.postForEntity(BASE_URL, new CreateUserRequest("test-get", "Test Get"), Map.class);
        assertStatus(HttpStatus.CREATED, created);

        var response = restTemplate.getForEntity(BASE_URL + "/test-get", Map.class);

        assertStatus(HttpStatus.OK, response);
        var body = response.getBody();
        assertNotNull(body);
        assertEquals("test-get", body.get("handle"));
        assertEquals("Test Get", body.get("display_name"));
    }

    @Test
    void getUser_selfView_includesBlocked() {
        restTemplate.postForEntity(BASE_URL, new CreateUserRequest("test-self", "Test Self"), Map.class);

        var headers = new HttpHeaders();
        headers.set("X-User-Handle", "test-self");
        var entity = new HttpEntity<>(headers);

        var response = restTemplate.exchange(BASE_URL + "/test-self", HttpMethod.GET, entity, Map.class);

        assertStatus(HttpStatus.OK, response);
        var body = response.getBody();
        assertNotNull(body);
        assertNotNull(body.get("blocked"));
    }

    // --- DELETE /api/v1/users/{handle} ---

    @Test
    void deleteUser_self_returns200AndRemovesUser() {
        var handle = "test-del-self";
        restTemplate.postForEntity(BASE_URL, new CreateUserRequest(handle, "Delete Self"), Map.class);

        var headers = new HttpHeaders();
        headers.set("X-User-Handle", handle);
        var entity = new HttpEntity<>(headers);

        var response = restTemplate.exchange(BASE_URL + "/" + handle, HttpMethod.DELETE, entity, String.class);

        assertStatus(HttpStatus.OK, response);
        assertEquals("User deleted", response.getBody());

        var getAfter = restTemplate.getForEntity(BASE_URL + "/" + handle, Map.class);
        assertStatus(HttpStatus.NOT_FOUND, getAfter);
    }

    @Test
    void deleteUser_withoutCallerHeader_returns401() {
        var response = restTemplate.exchange(BASE_URL + "/any-handle", HttpMethod.DELETE, HttpEntity.EMPTY, Map.class);

        assertStatus(HttpStatus.UNAUTHORIZED, response);
        var error = getErrorMap(response.getBody());
        assertEquals("unauthorized", error.get("code"));
    }

    @Test
    void deleteUser_wrongCaller_returns403() {
        var headers = new HttpHeaders();
        headers.set("X-User-Handle", "alice");
        var entity = new HttpEntity<>(headers);

        var response = restTemplate.exchange(BASE_URL + "/bob", HttpMethod.DELETE, entity, Map.class);

        assertStatus(HttpStatus.FORBIDDEN, response);
        var error = getErrorMap(response.getBody());
        assertEquals("forbidden", error.get("code"));
    }

    @Test
    void deleteUser_notFound_returns404() {
        var headers = new HttpHeaders();
        headers.set("X-User-Handle", "no-such-user");
        var entity = new HttpEntity<>(headers);

        var response = restTemplate.exchange(BASE_URL + "/no-such-user", HttpMethod.DELETE, entity, Map.class);

        assertStatus(HttpStatus.NOT_FOUND, response);
        var error = getErrorMap(response.getBody());
        assertEquals("not_found", error.get("code"));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getErrorMap(Map<String, Object> body) {
        assertNotNull(body);
        return (Map<String, Object>) body.get("error");
    }
}
