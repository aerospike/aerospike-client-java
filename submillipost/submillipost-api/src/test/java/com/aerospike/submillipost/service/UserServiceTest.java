package com.aerospike.submillipost.service;

import com.aerospike.submillipost.exception.InvalidUserHandleException;
import com.aerospike.submillipost.exception.UnavailableException;
import com.aerospike.submillipost.exception.UserAlreadyExistsException;
import com.aerospike.submillipost.exception.UserNotFoundException;
import com.aerospike.submillipost.model.User;
import com.aerospike.submillipost.model.UserStatus;
import com.aerospike.submillipost.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserServiceTest {

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private UserService userService;

    private User activeUser;

    @BeforeEach
    void setUp() {
        activeUser = new User("alice", "Alice", 0, 0, 0, 0,
                System.currentTimeMillis(), UserStatus.ACTIVE,
                List.of("blocked_user"), null, null);
    }

    // --- createUser ---

    @Test
    void createUser_success() {
        var expected = new User("alice", "Alice", 0, 0, 0, 0,
                1000L, UserStatus.ACTIVE, null, null, null);
        when(userRepository.create("alice", "Alice")).thenReturn(expected);

        var result = userService.createUser("alice", "Alice");

        assertNotNull(result);
        assertEquals("alice", result.handle());
        assertEquals("Alice", result.displayName());
        assertEquals(0, result.postCount());
        assertEquals(UserStatus.ACTIVE, result.status());
        verify(userRepository).create("alice", "Alice");
    }

    @Test
    void createUser_invalidHandle_throwsValidationError() {
        assertThrows(InvalidUserHandleException.class, () -> userService.createUser("alice@", "Alice"));
        assertThrows(InvalidUserHandleException.class, () -> userService.createUser("user.name", "User"));
        assertThrows(InvalidUserHandleException.class, () -> userService.createUser("user name", "User"));
        assertThrows(InvalidUserHandleException.class, () -> userService.createUser("", "User"));
        assertThrows(InvalidUserHandleException.class, () -> userService.createUser(null, "User"));

        // Valid formats should NOT throw
        when(userRepository.create(anyString(), anyString())).thenReturn(activeUser);
        assertDoesNotThrow(() -> userService.createUser("alice", "Alice"));
        assertDoesNotThrow(() -> userService.createUser("bob-123", "Bob"));
        assertDoesNotThrow(() -> userService.createUser("user_001", "User"));
    }

    @Test
    void createUser_duplicateHandle_throwsConflict() {
        when(userRepository.create("alice", "Alice")).thenThrow(new UserAlreadyExistsException("alice"));

        assertThrows(UserAlreadyExistsException.class, () -> userService.createUser("alice", "Alice"));
    }

    // --- getUser ---

    @Test
    void getUser_success() {
        when(userRepository.findByHandle("alice")).thenReturn(activeUser);

        var result = userService.getUser("alice", "alice");

        assertNotNull(result);
        assertEquals("alice", result.handle());
    }

    @Test
    void getUser_selfView_includesBlocked() {
        when(userRepository.findByHandle("alice")).thenReturn(activeUser);

        var result = userService.getUser("alice", "alice");

        assertNotNull(result.blocked());
        assertEquals(List.of("blocked_user"), result.blocked());
    }

    @Test
    void getUser_otherView_excludesBlocked() {
        when(userRepository.findByHandle("alice")).thenReturn(activeUser);

        var result = userService.getUser("alice", "bob");

        assertNull(result.blocked());
    }

    @Test
    void getUser_noCallerHandle_excludesBlocked() {
        when(userRepository.findByHandle("alice")).thenReturn(activeUser);

        var result = userService.getUser("alice", null);

        assertNull(result.blocked());
    }

    @Test
    void getUser_notFound_throwsNotFound() {
        when(userRepository.findByHandle("unknown")).thenReturn(null);

        assertThrows(UserNotFoundException.class, () -> userService.getUser("unknown", null));
    }

    @Test
    void getUser_deleting_throwsUnavailable() {
        var deletingUser = new User("alice", "Alice", 0, 0, 0, 0,
                1000L, UserStatus.DELETING, null, null, null);
        when(userRepository.findByHandle("alice")).thenReturn(deletingUser);

        assertThrows(UnavailableException.class, () -> userService.getUser("alice", null));
    }

    // --- updateUser ---

    @Test
    void updateUser_success() {
        var updatedUser = new User("alice", "Alice Updated", 0, 0, 0, 0,
                1000L, UserStatus.ACTIVE, null, null, null);
        when(userRepository.updateDisplayName("alice", "Alice Updated")).thenReturn(updatedUser);

        var result = userService.updateUser("alice", "Alice Updated");

        assertEquals("Alice Updated", result.displayName());
    }

    @Test
    void updateUser_notFound() {
        when(userRepository.updateDisplayName("alice", "New Name")).thenReturn(null);

        assertThrows(UserNotFoundException.class,
                () -> userService.updateUser("alice", "New Name"));
    }

    // --- deleteUser ---

    @Test
    void deleteUser_success() {
        when(userRepository.findByHandle("alice")).thenReturn(activeUser);
        when(userRepository.delete("alice")).thenReturn(true);

        assertDoesNotThrow(() -> userService.deleteUser("alice"));

        verify(userRepository).delete("alice");
    }

    @Test
    void deleteUser_notFound() {
        when(userRepository.findByHandle("alice")).thenReturn(null);

        assertThrows(UserNotFoundException.class,
                () -> userService.deleteUser("alice"));
    }
}
