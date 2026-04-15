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
import com.aerospike.submillipost.model.Post;
import com.aerospike.submillipost.model.PostStatus;
import org.springframework.stereotype.Repository;

import java.util.List;

import static com.aerospike.submillipost.repository.Bins.Post.*;

@Repository
public class PostRepository {

    private static final String SET = "posts";

    private final IAerospikeClient client;
    private final String namespace;

    public PostRepository(IAerospikeClient client, AppConfig appConfig) {
        this.client = client;
        this.namespace = appConfig.getAerospike().getNamespace();
    }

    public Post create(String postId,
                       String title,
                       String subtitle,
                       List<String> authors,
                       long pubDateMs,
                       String body) {
        var key = new Key(namespace, SET, postId);
        long createdAtMs = System.currentTimeMillis();

        var policy = new WritePolicy(client.getWritePolicyDefault());
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

        client.put(policy, key,
                new Bin(TITLE, title),
                new Bin(SUBTITLE, subtitle),
                new Bin(BODY, body),
                new Bin(AUTHORS, authors),
                new Bin(PUB_DATE_MS, pubDateMs),
                new Bin(LIKE_CNT, 0),
                new Bin(REPOST_CNT, 0),
                new Bin(CREATED_AT_MS, createdAtMs),
                new Bin(STATUS, PostStatus.ACTIVE.value()));

        return new Post(postId, title, subtitle, authors, pubDateMs, body,
                0, 0, createdAtMs, PostStatus.ACTIVE);
    }

    public Post findById(String postId) {
        var key = new Key(namespace, SET, postId);
        Record record = client.get(null, key);
        if (record == null) {
            return null;
        }
        return mapRecordToPost(postId, record);
    }

    public Post updateSubtitleAndBody(String postId, String subtitle, String body) {
        var key = new Key(namespace, SET, postId);
        var policy = new WritePolicy(client.getWritePolicyDefault());
        policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

        try {
            client.put(policy, key,
                    new Bin(SUBTITLE, subtitle),
                    new Bin(BODY, body));
            return findById(postId);
        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                return null;
            }
            throw e;
        }
    }

    public void markForDelete(String postId) {
        var key = new Key(namespace, SET, postId);
        var policy = new WritePolicy(client.getWritePolicyDefault());
        policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        client.put(policy, key, new Bin(STATUS, PostStatus.MARKED_FOR_DELETION.value()));
    }

    public boolean delete(String postId) {
        var key = new Key(namespace, SET, postId);
        return client.delete(null, key);
    }

    @SuppressWarnings("unchecked")
    private Post mapRecordToPost(String postId, Record record) {
        return new Post(
                postId,
                record.getString(TITLE),
                record.getString(SUBTITLE),
                (List<String>) record.getValue(AUTHORS),
                record.getLong(PUB_DATE_MS),
                record.getString(BODY),
                record.getInt(LIKE_CNT),
                record.getInt(REPOST_CNT),
                record.getLong(CREATED_AT_MS),
                PostStatus.fromValue(record.getString(STATUS))
        );
    }
}
