package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;

public final class AsyncQueryExecutor extends AsyncMultiExecutor {
	private final RecordSequenceListener listener;
	
	public AsyncQueryExecutor(
			AsyncCluster cluster,
			QueryPolicy policy,
			Statement statement,
			RecordSequenceListener listener
		) {
			this.listener = listener;
			
			Node[] nodes = cluster.getNodes();
			if (nodes.length == 0) {
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.");
			}

			completedSize = nodes.length;
			long taskId = System.nanoTime();

			for (Node node : nodes) {
				AsyncQuery async = new AsyncQuery(this, cluster, (AsyncNode)node, policy, statement, listener, taskId);
				async.execute();
			}
			
		}
	
	protected void onSuccess() {
		listener.onSuccess();
	}
	
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}
}
