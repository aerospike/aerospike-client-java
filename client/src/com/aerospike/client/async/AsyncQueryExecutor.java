package com.aerospike.client.async;

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;

public final class AsyncQueryExecutor extends AsyncMultiExecutor { 
	private final RecordSequenceListener listener;
	private final AsyncQuery[] threads;
	
	private final int total;
	private final AtomicInteger done;
	private final AtomicInteger executing;
	
	private final QueryPolicy policy;
	private long taskId;
	
	public AsyncQueryExecutor(
			AsyncCluster cluster,
			QueryPolicy policy,
			Statement statement,
			RecordSequenceListener listener
		) {
			this.policy = policy;
			this.listener = listener;
			taskId = System.nanoTime();
			statement.prepare();

			Node[] nodes = cluster.getNodes();
			if (nodes.length == 0) {
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.");
			}

			completedSize = nodes.length;
			taskId = System.nanoTime();

			threads = new AsyncQuery[nodes.length];
			total = nodes.length;
			int i = 0;
			for (Node node : nodes) {			
				threads[i++] = new AsyncQuery(this, cluster, (AsyncNode)node, policy, statement, listener, taskId);
			}

			done = new AtomicInteger();
			executing = new AtomicInteger();
			
			int firstStep = nodes.length;
			if (policy.maxConcurrentNodes != 0)
				firstStep = policy.maxConcurrentNodes;
				
			executing.getAndAdd(firstStep);
			for (i=0;i < firstStep; i++)
				threads[i].execute();
		}

	protected void onSuccess() {
		int realDone = done.addAndGet(executing.get());
		if (total <= realDone)
			listener.onSuccess();
		else {
			int nextMax = policy.maxConcurrentNodes;
			if (nextMax > (total-realDone)) nextMax = (total-realDone);
			executing.getAndAdd(nextMax);
			for (int i=realDone; i<realDone+nextMax;i++)
				threads[i].execute();
		}
	}

	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}

	
}
