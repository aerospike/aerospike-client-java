package com.aerospike.submillipost.service;

import com.aerospike.submillipost.exception.SelfBlockException;
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
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserBlockServiceTest {

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private UserBlockService blockService;

    private User alice;
    private User bob;

    @BeforeEach
    void setUp() {
        alice = new User("alice", "Alice", 0, 0, 0, 0,
                1000L, UserStatus.ACTIVE, null, null, null);
        bob = new User("bob", "Bob", 0, 0, 0, 0,
                1000L, UserStatus.ACTIVE, null, null, null);
    }

    @Test
    void block_success() {
        when(userRepository.findByHandle("alice")).thenReturn(alice);
        when(userRepository.findByHandle("bob")).thenReturn(bob);

        blockService.block("alice", "bob");

        verify(userRepository).addBlocked("alice", "bob");
    }

    @Test
    void block_self_throws() {
        assertThrows(SelfBlockException.class, () -> blockService.block("alice", "alice"));
        verify(userRepository, never()).addBlocked(any(), any());
    }

    @Test
    void block_ownerNotFound_throws() {
        when(userRepository.findByHandle("ghost")).thenReturn(null);

        assertThrows(UserNotFoundException.class, () -> blockService.block("ghost", "bob"));
        verify(userRepository, never()).addBlocked(any(), any());
    }

    @Test
    void block_targetNotFound_throws() {
        when(userRepository.findByHandle("alice")).thenReturn(alice);
        when(userRepository.findByHandle("ghost")).thenReturn(null);

        assertThrows(UserNotFoundException.class, () -> blockService.block("alice", "ghost"));
        verify(userRepository, never()).addBlocked(any(), any());
    }

    @Test
    void unblock_success() {
        when(userRepository.findByHandle("alice")).thenReturn(alice);

        blockService.unblock("alice", "bob");

        verify(userRepository).removeBlocked("alice", "bob");
    }

    @Test
    void unblock_ownerNotFound_throws() {
        when(userRepository.findByHandle("ghost")).thenReturn(null);

        assertThrows(UserNotFoundException.class, () -> blockService.unblock("ghost", "bob"));
        verify(userRepository, never()).removeBlocked(any(), any());
    }

    @Test
    void listBlocked_returnsList() {
        when(userRepository.listBlocked("alice")).thenReturn(List.of("bob", "eve"));

        var result = blockService.listBlocked("alice");

        assertEquals(List.of("bob", "eve"), result);
    }

    @Test
    void listBlocked_emptyList() {
        when(userRepository.listBlocked("alice")).thenReturn(List.of());

        assertTrue(blockService.listBlocked("alice").isEmpty());
    }

    @Test
    void listBlocked_userNotFound_throws() {
        when(userRepository.listBlocked("ghost")).thenReturn(null);

        assertThrows(UserNotFoundException.class, () -> blockService.listBlocked("ghost"));
    }

    @Test
    void isBlocked_By_delegates() {
        when(userRepository.isBlocked("alice", "bob")).thenReturn(true);
        when(userRepository.isBlocked("alice", "eve")).thenReturn(false);

        assertTrue(blockService.isBlockedBy("alice", "bob"));
        assertFalse(blockService.isBlockedBy("alice", "eve"));
    }
}
