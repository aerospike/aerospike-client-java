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
    private static final String INDEX_NAME = "index-1";

    @Override
    @Before
    public void setUp() throws Exception {
        if (indexExists(TestQueryEngine.NAMESPACE, INDEX_NAME)) {
            wait(client.dropIndex(null, TestQueryEngine.NAMESPACE, SET, INDEX_NAME));
        }
        super.setUp();
    }

    @Test
    public void refreshIndexes_findsNewlyCreatedIndex() throws Exception {
        Optional<Index> index = queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_1));
        assertThat(index).isEmpty();

        wait(client.createIndex(null, TestQueryEngine.NAMESPACE, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC));

        queryEngine.refreshIndexes();

        index = queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_1));
        assertThat(index).isPresent();
    }

    @Test
    public void refreshIndexes_removesDeletedIndex() throws Exception {
        wait(client.createIndex(null, TestQueryEngine.NAMESPACE, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC));

        queryEngine.refreshIndexes();

        assertThat(queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_1))).isPresent();

        wait(client.dropIndex(null, TestQueryEngine.NAMESPACE, SET, INDEX_NAME));

        queryEngine.refreshIndexes();

        assertThat(queryEngine.getIndex(new IndexKey(TestQueryEngine.NAMESPACE, SET, BIN_1))).isEmpty();
    }

    @Test
    public void getIndex_returnsEmptyForNonExistingIndex() throws Exception {
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
