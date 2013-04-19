/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchExistsSequenceExecutor extends AsyncBatchExecutor {
	private final ExistsSequenceListener listener;

	public AsyncBatchExistsSequenceExecutor(
		AsyncCluster cluster,
		Policy policy, 
		Key[] keys,
		ExistsSequenceListener listener
	) throws AerospikeException {
		super(cluster, keys);
		this.listener = listener;
		
		// Dispatch asynchronous commands to nodes.
		for (BatchNode batchNode : batchNodes) {			
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
				Command command = new Command();
				command.setBatchExists(batchNamespace);
				
				AsyncBatchExistsSequence async = new AsyncBatchExistsSequence(this, cluster, (AsyncNode)batchNode.node, listener);
				async.execute(policy, command);
			}
		}
	}
	
	protected void onSuccess() {
		listener.onSuccess();
	}
	
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}		
}
