package com.aerospike.helper.query.cache;

import com.aerospike.helper.model.Index;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

/**
 * Internal index related operations used by ReactorIndexCache and IndexCache.
 */
public class InternalIndexOperations {

    private static final String SINDEX = "sindex";

    private final IndexInfoParser indexInfoParser;

    public InternalIndexOperations(IndexInfoParser indexInfoParser) {
        this.indexInfoParser = indexInfoParser;
    }

    public Cache parseIndexesInfo(String infoResponse) {
        if (infoResponse.isEmpty()) {
            return Cache.empty();
        }
        return Cache.of(Arrays.stream(infoResponse.split(";"))
                .map(indexInfoParser::parse)
                .collect(collectingAndThen(
                        toMap(InternalIndexOperations::getIndexKey, index -> index),
                        Collections::unmodifiableMap)));
    }

    public String buildGetIndexesCommand() {
        return SINDEX;
    }

    private static IndexKey getIndexKey(Index index) {
        return new IndexKey(index.getNamespace(), index.getSet(), index.getBin(), index.getType());
    }

    public static class Cache {

        private static final Cache EMPTY = new Cache(Collections.emptyMap());

        public final Map<IndexKey, Index> indexes;
        public final Set<IndexedField> indexedFields;

        private Cache(Map<IndexKey, Index> indexes) {
            this.indexes = Collections.unmodifiableMap(indexes);
            this.indexedFields = indexes.keySet().stream()
                    .map(key -> new IndexedField(key.getNamespace(), key.getSet(), key.getField()))
                    .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
        }

        public static Cache empty() {
            return EMPTY;
        }

        public static Cache of(Map<IndexKey, Index> cache) {
            return new Cache(cache);
        }
    }
}