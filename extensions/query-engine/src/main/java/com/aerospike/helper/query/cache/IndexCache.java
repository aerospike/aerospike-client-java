package com.aerospike.helper.query.cache;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.helper.model.Index;
import com.aerospike.helper.query.QueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexCache implements AutoCloseable {

    private static final String SINDEX = "sindex";
    private volatile Map<IndexKey, Index> indexCache = Collections.emptyMap();

    private final Logger log = LoggerFactory.getLogger(QueryEngine.class);
    private final AerospikeClient client;
    private final InfoPolicy infoPolicy;

    public IndexCache(AerospikeClient client, InfoPolicy infoPolicy) {
        this.client = client;
        this.infoPolicy = infoPolicy;
    }

    public Optional<Index> getIndex(IndexKey indexKey) {
        return Optional.ofNullable(this.indexCache.get(indexKey));
    }

    public void refreshIndexes() {
        log.trace("Loading indexes");
        this.indexCache = Arrays.stream(client.getNodes())
                .filter(Node::isActive)
                .findFirst()
                .map(node -> Info.request(infoPolicy, node, SINDEX))
                .filter(indexString -> !indexString.isEmpty())
                .map(indexString -> Arrays.stream(indexString.split(";")))
                .orElse(Stream.empty())
                .map(Index::new)
                .collect(Collectors.toMap(this::getIndexKey, index -> index));

        log.debug("Loaded indexes: {}", indexCache);
    }

    @Override
    public void close() {
        this.indexCache = Collections.emptyMap();
    }

    private IndexKey getIndexKey(Index index) {
        return new IndexKey(index.getNamespace(), index.getSet(), index.getBin());
    }
}
