package com.aerospike.helper.query.cache;

import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import com.aerospike.helper.model.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Optional;

import static com.aerospike.helper.query.cache.InternalIndexOperations.Cache;

public class ReactorIndexCache implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReactorIndexCache.class);

    private volatile Cache cache = Cache.empty();
    private final IAerospikeReactorClient client;
    private final InfoPolicy infoPolicy;
    private final InternalIndexOperations indexOperations;

    public ReactorIndexCache(IAerospikeReactorClient client, InfoPolicy infoPolicy, InternalIndexOperations indexOperations) {
        this.client = client;
        this.infoPolicy = infoPolicy;
        this.indexOperations = indexOperations;
    }

    public Optional<Index> getIndex(IndexKey indexKey) {
        return Optional.ofNullable(this.cache.indexes.get(indexKey));
    }

    public boolean hasIndexFor(IndexedField indexedField) {
        return this.cache.indexedFields.contains(indexedField);
    }

    public Mono<Void> refreshIndexes() {
        return client.info(infoPolicy, null, indexOperations.buildGetIndexesCommand())
                .doOnSubscribe(subscription -> log.trace("Loading indexes"))
                .doOnNext(indexInfo -> {
                    this.cache = indexOperations.parseIndexesInfo(indexInfo);
                    log.debug("Loaded indexes: {}", cache.indexes);
                }).then();
    }

    @Override
    public void close() {
        this.cache = Cache.empty();
    }
}
