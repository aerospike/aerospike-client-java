package com.aerospike;


import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeContainerTest {

    @ClassRule
    public static AerospikeContainer container = new AerospikeContainer("aerospike/aerospike-server:4.3.0.2");

    @Test
    public void shouldStartContainer() {
        AerospikeClient client = container.getClient();

        Key key = new Key(container.getNamespace(), "test-set", "test-key");

        client.add(null, key, new Bin("k1", "v1"));

        Record actual = client.get(null, key);

        assertThat(actual).isNotNull();
        assertThat(actual.bins).containsEntry("k1", "v1");
    }
}