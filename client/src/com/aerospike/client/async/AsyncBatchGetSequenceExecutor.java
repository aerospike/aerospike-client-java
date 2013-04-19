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

import java.util.HashSet;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchGetSequenceExecutor extends AsyncBatchExecutor {
	private final RecordSequenceListener listener;

	public AsyncBatchGetSequenceExecutor(
		AsyncCluster cluster,
		Policy policy, 
		RecordSequenceListener listener,
		Key[] keys,
		HashSet<String> binNames,
		int readAttr
	) throws AerospikeException {
		super(cluster, keys);
		this.listener = listener;
		
		// Dispatch asynchronous commands to nodes.
		for (BatchNode batchNode : batchNodes) {			
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
				Command command = new Command();
				command.setBatchGet(batchNamespace, binNames, readAttr);
				
				AsyncBatchGetSequence async = new AsyncBatchGetSequence(this, cluster, (AsyncNode)batchNode.node, binNames, listener);
				async.execute(policy, command);
			}
		}
	}
	
	@Override
	protected void onSuccess() {
		listener.onSuccess();
	}
	
	@Override
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}		
}
