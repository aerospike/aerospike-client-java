package com.aerospike.submillipost.service;

import com.aerospike.submillipost.exception.InvalidUserHandleException;
import com.aerospike.submillipost.exception.UnavailableException;
import com.aerospike.submillipost.exception.UserNotFoundException;
import com.aerospike.submillipost.model.User;
import com.aerospike.submillipost.model.UserStatus;
import com.aerospike.submillipost.repository.UserRepository;
import org.springframework.stereotype.Service;

import java.util.regex.Pattern;

@Service
public class UserService {

    private static final Pattern HANDLE_PATTERN = Pattern.compile("^[A-Za-z0-9_-]+$");

    private final UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public User createUser(String handle, String displayName) {
        validateHandle(handle);
        return userRepository.create(handle, displayName);
    }

    public User getUser(String handle, String callerHandle) {
        var user = userRepository.findByHandle(handle);
        if (user == null) {
            throw new UserNotFoundException(handle);
        }
        if (user.status() == UserStatus.DELETING) {
            throw new UnavailableException("User is being deleted: " + handle);
        }

        // blocked list to be added only for self
        if (callerHandle == null || !callerHandle.equals(handle)) {
            return new User(
                    user.handle(), user.displayName(), user.postCount(), user.noteCount(),
                    user.followerCount(), user.followingCount(), user.createdAtMs(),
                    user.status(), null, user.postIds(), user.noteIds()
            );
        }
        return user;
    }

    public User updateUser(String handle, String displayName) {
        var user = userRepository.updateDisplayName(handle, displayName);
        if (user == null) {
            throw new UserNotFoundException(handle);
        }
        return user;
    }

    public void deleteUser(String handle) {
        var user = userRepository.findByHandle(handle);
        if (user == null) {
            throw new UserNotFoundException(handle);
        }

        // Cascade logic added at later levels.
        userRepository.delete(handle);
    }

    private void validateHandle(String handle) {
        if (handle == null || !HANDLE_PATTERN.matcher(handle).matches()) {
            throw new InvalidUserHandleException(handle == null ? "" : handle);
        }
    }
}
