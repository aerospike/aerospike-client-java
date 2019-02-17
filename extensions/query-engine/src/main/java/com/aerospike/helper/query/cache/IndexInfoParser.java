package com.aerospike.helper.query.cache;

import com.aerospike.client.query.IndexType;
import com.aerospike.helper.model.Index;

import java.util.*;
import java.util.stream.Collectors;

public class IndexInfoParser {

    private static final String INDEX_NAME = "indexname";
    private static final String BIN = "bin";
    private static final String BINS = "bins";
    private static final String SET = "set";
    private static final String NS = "ns";
    private static final String NAMESPACE = "namespace";
    private static final String TYPE = "type";
    private static final String STRING_TYPE = "STRING";
    private static final String GEO_JSON_TYPE = "GEOJSON";
    private static final String NUMERIC_TYPE = "NUMERIC";

    public Index parse(String infoString) {
        Map<String, String> values = getIndexInfo(infoString);
        String name = getRequiredName(values);
        String namespace = getRequiredNamespace(values);
        String set = getNullableSet(values);
        String bin = getRequiredBin(values);
        IndexType indexType = getIndexTypeInternal(values);
        return new Index(values, name, namespace, set, bin, indexType);
    }

    /**
     * Populates the Index object from an "info" message from Aerospike
     *
     * @param info Info string from node
     */
    private Map<String, String> getIndexInfo(String info) {
        //ns=phobos_sindex:set=longevity:indexname=str_100_idx:num_bins=1:bins=str_100_bin:type=TEXT:sync_state=synced:state=RW;
        //ns=test:set=Customers:indexname=mail_index_userss:bin=email:type=STRING:indextype=LIST:path=email:sync_state=synced:state=RW
        if (info.isEmpty()) {
            return Collections.emptyMap();
        }
        return Arrays.stream(info.split(":"))
                .map(part -> {
                    String[] kvParts = part.split("=");
                    if (kvParts.length == 0) {
                        throw new IllegalStateException("Failed to parse info string part: " + part);
                    }
                    if (kvParts.length == 1) {
                        return null;
                    }
                    return new AbstractMap.SimpleEntry<>(kvParts[0], kvParts[1]);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    private String getRequiredName(Map<String, String> values) {
        String name = values.get(INDEX_NAME);
        if (name != null)
            return name;
        throw new IllegalStateException("Name not present in info: " + values);
    }

    private IndexType getIndexTypeInternal(Map<String, String> values) {
        String indexTypeString = values.get(TYPE);
        if (indexTypeString == null) {
            throw new IllegalStateException("Index type not present in info: " + values);
        }
        if (indexTypeString.equalsIgnoreCase(STRING_TYPE))
            return IndexType.STRING;
        else if (indexTypeString.equalsIgnoreCase(GEO_JSON_TYPE))
            return IndexType.GEO2DSPHERE;
        else if (indexTypeString.equalsIgnoreCase(NUMERIC_TYPE))
            return IndexType.NUMERIC;
        return null;
//        TODO: should we throw exception in case of unknown index type?
//        throw new IllegalStateException("Unknown index type: " + indexTypeString + " in info: " + values);
    }

    private String getRequiredBin(Map<String, String> values) {
        String bin = values.get(BIN);
        if (bin != null)
            return bin;
        String bins = values.get(BINS);
        if (bins != null)
            return bins;
        throw new IllegalStateException("Bin not present in info: " + values);
    }

    private String getNullableSet(Map<String, String> values) {
        String set = values.get(SET);
        if (set != null && set.equalsIgnoreCase("null")) {
            return null;
        }
        return set;
    }

    private String getRequiredNamespace(Map<String, String> values) {
        String ns = values.get(NS);
        if (ns != null)
            return ns;
        String namespace = values.get(NAMESPACE);
        if (namespace != null)
            return namespace;
        throw new IllegalStateException("Namespace not present in info: " + values);
    }
}
