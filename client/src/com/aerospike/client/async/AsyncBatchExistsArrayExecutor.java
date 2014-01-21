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

import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.command.BatchItem;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchExistsArrayExecutor extends AsyncBatchExecutor {
	private final ExistsArrayListener listener;
	private final boolean[] existsArray;

	public AsyncBatchExistsArrayExecutor(
		AsyncCluster cluster,
		Policy policy, 
		Key[] keys,
		ExistsArrayListener listener
	) throws AerospikeException {
		super(cluster, keys);
		this.existsArray = new boolean[keys.length];
		this.listener = listener;
		
		if (policy == null) {
			policy = new Policy();
		}
		
		HashMap<Key,BatchItem> keyMap = BatchItem.generateMap(keys);
		
		// Dispatch asynchronous commands to nodes.
		for (BatchNode batchNode : batchNodes) {			
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
				AsyncBatchExistsArray async = new AsyncBatchExistsArray(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keyMap, existsArray);
				async.execute();
			}
		}
	}
	
	protected void onSuccess() {
		listener.onSuccess(keys, existsArray);
	}
	
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}		
}
