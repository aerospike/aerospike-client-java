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
import java.util.HashSet;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.BatchItem;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchGetArrayExecutor extends AsyncBatchExecutor {
	private final RecordArrayListener listener;
	private final Record[] recordArray;

	public AsyncBatchGetArrayExecutor(
		AsyncCluster cluster,
		Policy policy, 
		RecordArrayListener listener,
		Key[] keys,
		HashSet<String> binNames,
		int readAttr
	) throws AerospikeException {
		super(cluster, keys);
		this.recordArray = new Record[keys.length];
		this.listener = listener;
		
		HashMap<Key,BatchItem> keyMap = BatchItem.generateMap(keys);
		
		// Dispatch asynchronous commands to nodes.
		for (BatchNode batchNode : batchNodes) {			
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
				Command command = new Command();
				command.setBatchGet(batchNamespace, binNames, readAttr);
				
				AsyncBatchGetArray async = new AsyncBatchGetArray(this, cluster, (AsyncNode)batchNode.node, keyMap, binNames, recordArray);
				async.execute(policy, command);
			}
		}
	}
	
	protected void onSuccess() {
		listener.onSuccess(keys, recordArray);
	}
	
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}		
}
