package com.aerospike.helper.query.cache;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.helper.model.Index;
import com.aerospike.helper.query.QueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

public class IndexCache implements AutoCloseable {

    private static final String SINDEX = "sindex";
    private volatile Map<IndexKey, Index> indexCache = Collections.emptyMap();

    private final Logger log = LoggerFactory.getLogger(QueryEngine.class);
    private final AerospikeClient client;
    private final InfoPolicy infoPolicy;
    private final IndexInfoParser indexInfoParser;

    public IndexCache(AerospikeClient client, InfoPolicy infoPolicy, IndexInfoParser indexInfoParser) {
        this.client = client;
        this.infoPolicy = infoPolicy;
        this.indexInfoParser = indexInfoParser;
    }

    public Optional<Index> getIndex(IndexKey indexKey) {
        return Optional.ofNullable(this.indexCache.get(indexKey));
    }

    public void refreshIndexes() {
        log.trace("Loading indexes");
        this.indexCache = Arrays.stream(client.getNodes())
                .filter(Node::isActive)
                .findFirst()
                .map(node -> Info.request(infoPolicy, node, buildGetIndexesCommand()))
                .map(response -> parseIndexesInfo(response, indexInfoParser))
                .orElse(Collections.emptyMap());

        log.debug("Loaded indexes: {}", indexCache);
    }

    static Map<IndexKey, Index> parseIndexesInfo(String infoResponse, IndexInfoParser indexInfoParser){
        if(infoResponse.isEmpty()){
            return Collections.emptyMap();
        }
        return Arrays.stream(infoResponse.split(";"))
                .map(indexInfoParser::parse)
                .collect(collectingAndThen(
                        toMap(IndexCache::buildIndexKey, index -> index),
                        Collections::unmodifiableMap));
    }

    public static String buildGetIndexesCommand(){
        return SINDEX;
    }

    @Override
    public void close() {
        this.indexCache = Collections.emptyMap();
    }

    private static IndexKey buildIndexKey(Index index) {
        return new IndexKey(index.getNamespace(), index.getSet(), index.getBin());
    }
}
