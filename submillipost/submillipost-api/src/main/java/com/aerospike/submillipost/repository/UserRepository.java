package com.aerospike.submillipost.repository;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.submillipost.config.AppConfig;
import com.aerospike.submillipost.exception.UserAlreadyExistsException;
import com.aerospike.submillipost.model.User;
import com.aerospike.submillipost.model.UserStatus;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.aerospike.submillipost.repository.Bins.User.*;

@Repository
public class UserRepository {

    private static final String SET = "users";

    private final IAerospikeClient client;
    private final String namespace;

    public UserRepository(IAerospikeClient client, AppConfig appConfig) {
        this.client = client;
        this.namespace = appConfig.getAerospike().getNamespace();
    }

    public User create(String handle, String displayName) {
       var key = new Key(namespace, SET, handle);
       long createdAtMs = System.currentTimeMillis();

       var policy = new WritePolicy(client.getWritePolicyDefault());
       policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
       try {
           client.put(policy, key,
                   new Bin(DISPLAY_NAME, displayName),
                   new Bin(POST_CNT, 0),
                   new Bin(NOTE_CNT, 0),
                   new Bin(FOLLOWING_CNT, 0),
                   new Bin(FOLLOWER_CNT, 0),
                   new Bin(CREATED_AT_MS, createdAtMs),
                   new Bin(STATUS, UserStatus.ACTIVE.value()));

           return new User(handle, displayName, 0, 0, 0, 0, createdAtMs, UserStatus.ACTIVE,
                   null, null, null);
       } catch (AerospikeException e) {
           if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
               throw new UserAlreadyExistsException(handle);
           }
           throw e;
       }
    }

    public User findByHandle(String handle) {
        var key = new Key(namespace, SET, handle);
        Record record = client.get(null, key);

        if (record == null) {
            return null;
        }

        return mapRecordToUser(handle, record);
    }

    public User updateDisplayName(String handle, String displayName) {
        var key = new Key(namespace, SET, handle);
        var policy = new WritePolicy(client.getWritePolicyDefault());
        policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

        try {
            client.put(policy, key, new Bin(DISPLAY_NAME, displayName));
            return findByHandle(handle);
        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                return null;
            }
            throw e;
        }
    }

    public boolean delete(String handle) {
        var key = new Key(namespace, SET, handle);
        return client.delete(null, key);
    }

    @SuppressWarnings("unchecked")
    private User mapRecordToUser(String handle, Record record) {
        return new User(
                handle,
                record.getString(DISPLAY_NAME),
                record.getInt(POST_CNT),
                record.getInt(NOTE_CNT),
                record.getInt(FOLLOWER_CNT),
                record.getInt(FOLLOWING_CNT),
                record.getLong(CREATED_AT_MS),
                UserStatus.valueOf(record.getString(STATUS).toUpperCase()),
                (List<String>) Optional.ofNullable(record.getValue(BLOCKED)).orElse(Collections.emptyList()),
                (List<String>) record.getValue(POST_IDS),
                (List<String>) record.getValue(NOTE_IDS)
        );
    }
}
