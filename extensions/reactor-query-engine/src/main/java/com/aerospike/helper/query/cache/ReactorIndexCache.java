package com.aerospike.helper.query.cache;

import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import com.aerospike.helper.model.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static com.aerospike.helper.query.cache.IndexCache.buildGetIndexesCommand;
import static com.aerospike.helper.query.cache.IndexCache.parseIndexesInfo;

public class ReactorIndexCache implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReactorIndexCache.class);

    private final IAerospikeReactorClient client;
    private final InfoPolicy infoPolicy;
    private final IndexInfoParser indexInfoParser;

    private volatile Map<IndexKey, Index> indexCache = Collections.emptyMap();

    public ReactorIndexCache(IAerospikeReactorClient client, InfoPolicy infoPolicy, IndexInfoParser indexInfoParser) {
        this.client = client;
        this.infoPolicy = infoPolicy;
        this.indexInfoParser = indexInfoParser;
    }

    public Optional<Index> getIndex(IndexKey indexKey) {
        return Optional.ofNullable(this.indexCache.get(indexKey));
    }

    public Mono<Void> refreshIndexes() {
        return client.info(infoPolicy, null, buildGetIndexesCommand())
                .doOnSubscribe(subscription -> log.trace("Loading indexes"))
                .doOnNext(indexInfo ->  {
                    this.indexCache = parseIndexesInfo(indexInfo, indexInfoParser);
                    log.debug("Loaded indexes: {}", indexCache);
                }).then();
    }

    @Override
    public void close() {
        this.indexCache = Collections.emptyMap();
    }
}
