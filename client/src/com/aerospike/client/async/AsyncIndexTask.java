package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.listener.TaskStatusListener;
import com.aerospike.client.policy.InfoPolicy;

import java.util.Map;

import static com.aerospike.client.task.IndexTask.*;

public class AsyncIndexTask {

    private final IAerospikeClient client;
    private final String namespace;
    private final String indexName;
    private final boolean isCreate;

    /**
     * Initialize task with fields needed to query server nodes.
     */
    public AsyncIndexTask(IAerospikeClient client,
                          String namespace, String indexName, boolean isCreate) {
        this.client = client;
        this.namespace = namespace;
        this.indexName = indexName;
        this.isCreate = isCreate;
    }

    /**
     * Asynchronously query node for task completion status.
     * All nodes must respond with load_pct of 100 to be considered done.
     */
    public void queryStatus(EventLoop eventLoop, InfoPolicy policy, Node node, TaskStatusListener listener) throws AerospikeException {

        if (client.getNodes().length == 0) {
            listener.onFailure(new AerospikeException("Cluster is empty"));
        }

        String command = buildStatusCommand(namespace, indexName);

        client.info(eventLoop, new InfoListener() {
            @Override
            public void onSuccess(Map<String, String> map) {
                try {
                    int status = parseStatusResponse(command, map.values().iterator().next(), isCreate);
                    listener.onSuccess(status);
                } catch (AerospikeException ae) {
                    listener.onFailure(ae);
                }
            }

            @Override
            public void onFailure(AerospikeException ae) {
                listener.onFailure(ae);
            }
        }, policy, node, command);
    }

}
