package com.aerospike.helper.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.aerospike.helper.model.Index;
import com.aerospike.helper.query.cache.IndexKey;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class IndexTests extends AerospikeAwareTests {

    private static final String SET = "index-test";
    private static final String BIN_1 = "bin-1";
    private static final String BIN_2 = "bin-2";
    private static final String BIN_3 = "bin-3";
    private static final String INDEX_NAME = "index-1";
    private static final String INDEX_NAME_2 = "index-2";
    private static final String INDEX_NAME_3 = "index-3";

    @Override
    @Before
    public void setUp() throws Exception {
        dropIndexIfExists(INDEX_NAME, TestQueryEngine.NAMESPACE, SET);
        dropIndexIfExists(INDEX_NAME_2, TestQueryEngine.NAMESPACE, null);
        dropIndexIfExists(INDEX_NAME_3, TestQueryEngine.NAMESPACE, SET);
        super.setUp();
    }

    private void dropIndexIfExists(String indexName, String namespace, String set) {
        if (indexExists(TestQueryEngine.NAMESPACE, indexName)) {
            wait(client.dropIndex(null, namespace, set, indexName));
        }
    }

    @Test
    public void refreshIndexes_findsNewlyCreatedIndex() {
        Optional<Index> index = queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_1));
        assertThat(index).isEmpty();

        wait(client.createIndex(null, TestQueryEngine.NAMESPACE, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC));

        queryEngine.refreshIndexes();

        index = queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_1));
        assertThat(index).isPresent()
                .hasValueSatisfying(value -> {
                    assertThat(value.getName()).isEqualTo(INDEX_NAME);
                    assertThat(value.getNamespace()).isEqualTo(TestQueryEngine.NAMESPACE);
                    assertThat(value.getSet()).isEqualTo(SET);
                    assertThat(value.getBin()).isEqualTo(BIN_1);
                    assertThat(value.getType()).isEqualTo(IndexType.NUMERIC);
                });
    }

    @Test
    public void refreshIndexes_removesDeletedIndex() {
        wait(client.createIndex(null, TestQueryEngine.NAMESPACE, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC));

        queryEngine.refreshIndexes();

        assertThat(queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_1))).isPresent();

        wait(client.dropIndex(null, TestQueryEngine.NAMESPACE, SET, INDEX_NAME));

        queryEngine.refreshIndexes();

        assertThat(queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_1))).isEmpty();
    }

    @Test
    public void refreshIndexes_indexWithoutSetCanBeParsed() {
        wait(client.createIndex(null, TestQueryEngine.NAMESPACE, null, INDEX_NAME_2, BIN_2, IndexType.STRING));

        queryEngine.refreshIndexes();

        Optional<Index> index = queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, null, BIN_2));
        assertThat(index).isPresent()
                .hasValueSatisfying(value -> {
                    assertThat(value.getName()).isEqualTo(INDEX_NAME_2);
                    assertThat(value.getNamespace()).isEqualTo(TestQueryEngine.NAMESPACE);
                    assertThat(value.getSet()).isNull();
                    assertThat(value.getBin()).isEqualTo(BIN_2);
                    assertThat(value.getType()).isEqualTo(IndexType.STRING);
                });
    }

    @Test
    public void refreshIndexes_indexWithGeoTypeCanBeParsed() {
        wait(client.createIndex(null, TestQueryEngine.NAMESPACE, SET, INDEX_NAME_3, BIN_3, IndexType.GEO2DSPHERE));

        queryEngine.refreshIndexes();

        Optional<Index> index = queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_3));
        assertThat(index).isPresent()
                .hasValueSatisfying(value -> {
                    assertThat(value.getName()).isEqualTo(INDEX_NAME_3);
                    assertThat(value.getNamespace()).isEqualTo(TestQueryEngine.NAMESPACE);
                    assertThat(value.getSet()).isEqualTo(SET);
                    assertThat(value.getBin()).isEqualTo(BIN_3);
                    assertThat(value.getType()).isEqualTo(IndexType.GEO2DSPHERE);
                });
    }

    @Test
    public void refreshIndexes_multipleIndexesCanBeParsed() {
        wait(client.createIndex(null, TestQueryEngine.NAMESPACE, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC));
        wait(client.createIndex(null, TestQueryEngine.NAMESPACE, null, INDEX_NAME_2, BIN_2, IndexType.STRING));
        wait(client.createIndex(null, TestQueryEngine.NAMESPACE, SET, INDEX_NAME_3, BIN_3, IndexType.GEO2DSPHERE));

        queryEngine.refreshIndexes();

        assertThat(queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_1))).isPresent();
        assertThat(queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, null, BIN_2))).isPresent();
        assertThat(queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_3))).isPresent();
        assertThat(queryEngine.getIndex(new IndexKey("unknown", null, "unknown"))).isEmpty();
    }

    @Test
    public void getIndex_returnsEmptyForNonExistingIndex() {
        Optional<Index> index = queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_1));
        assertThat(index).isEmpty();
    }

    private static void wait(IndexTask task) {
        if (task == null) {
            throw new IllegalStateException("task can not be null");
        }
        task.waitTillComplete();
    }

    private boolean indexExists(String namespace, String indexName) {
        Node[] nodes = client.getNodes();
        if (nodes.length == 0) {
            throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
        }
        Node node = nodes[0];
        String response = Info.request(node, "sindex/" + namespace + '/' + indexName);
        return !response.startsWith("FAIL:201");
    }
}
