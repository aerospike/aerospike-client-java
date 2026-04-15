package com.aerospike.submillipost.service;

import com.aerospike.submillipost.exception.SelfBlockException;
import com.aerospike.submillipost.exception.UserNotFoundException;
import com.aerospike.submillipost.repository.UserRepository;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
public class UserBlockService {

    private final UserRepository userRepository;

    public UserBlockService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public void block(String ownerHandle, String targetHandle) {
       if (ownerHandle.equals(targetHandle)) {
           throw new SelfBlockException();
       }
       if (userRepository.findByHandle(ownerHandle) == null) {
           throw new UserNotFoundException(ownerHandle);
       }
       if (userRepository.findByHandle(targetHandle) == null) {
           throw new UserNotFoundException(targetHandle);
       }
       userRepository.addBlocked(ownerHandle, targetHandle);
    }

    public void unblock(String ownerHandle, String targetHandle) {
        if (userRepository.findByHandle(ownerHandle) == null) {
            throw new UserNotFoundException(ownerHandle);
        }
        userRepository.removeBlocked(ownerHandle, targetHandle);
    }

    public List<String> listBlocked(String ownerHandle) {
        var blocked = userRepository.listBlocked(ownerHandle);
        if (blocked == null) {
            throw new UserNotFoundException(ownerHandle);
        }
        return Collections.unmodifiableList(blocked);
    }

    /** True if owner has blocked candidate. False if owner doesn't exist. */
    public boolean isBlockedBy(String ownerHandle, String candidateHandle) {
        return userRepository.isBlocked(ownerHandle, candidateHandle);
    }
}
