package com.aerospike.helper.query;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import org.junit.After;
import org.junit.Before;

public abstract class AerospikeAwareTests {

    protected AerospikeClient client;
    protected ClientPolicy clientPolicy;
    protected QueryEngine queryEngine;

    public AerospikeAwareTests(){
        clientPolicy = new ClientPolicy();
        clientPolicy.timeout = TestQueryEngine.TIME_OUT;
        client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
        client.writePolicyDefault.expiration = 1800;
        client.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;

    }
    @Before
    public void setUp() {
        queryEngine = new QueryEngine(client);
    }

    @After
    public void tearDown() throws Exception {
        queryEngine.close();
    }
}
