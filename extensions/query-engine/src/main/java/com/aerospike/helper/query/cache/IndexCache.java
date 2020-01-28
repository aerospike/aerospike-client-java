package com.aerospike.helper.query.cache;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.helper.model.Index;
import com.aerospike.helper.query.cache.InternalIndexOperations.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

public class IndexCache implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(IndexCache.class);

    private volatile Cache cache = Cache.empty();
    private final AerospikeClient client;
    private final InfoPolicy infoPolicy;
    private final InternalIndexOperations indexOperations;

    public IndexCache(AerospikeClient client, InfoPolicy infoPolicy, InternalIndexOperations indexOperations) {
        this.client = client;
        this.infoPolicy = infoPolicy;
        this.indexOperations = indexOperations;
    }

    public Optional<Index> getIndex(IndexKey indexKey) {
        return Optional.ofNullable(this.cache.indexes.get(indexKey));
    }

    public boolean hasIndexFor(IndexedField indexedField) {
        return cache.indexedFields.contains(indexedField);
    }

    public void refreshIndexes() {
        log.trace("Loading indexes");
        this.cache = Arrays.stream(client.getNodes())
                .filter(Node::isActive)
                .findAny() // we do want to send info request to the random node (sending request to the first node may lead to uneven request distribution)
                .map(node -> Info.request(infoPolicy, node, indexOperations.buildGetIndexesCommand()))
                .map(response -> indexOperations.parseIndexesInfo(response))
                .orElse(Cache.empty());

        log.debug("Loaded indexes: {}", cache.indexes);
    }

    @Override
    public void close() {
        this.cache = Cache.empty();
    }

}
