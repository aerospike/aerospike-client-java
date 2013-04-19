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

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchNode;

public abstract class AsyncBatchExecutor extends AsyncMultiExecutor {
	protected final Key[] keys;
	protected final List<BatchNode> batchNodes;

	public AsyncBatchExecutor(Cluster cluster, Key[] keys) throws AerospikeException {
		this.keys = keys;	
		this.batchNodes = BatchNode.generateList(cluster, keys);
		
		// Count number of asynchronous commands needed.
		int size = 0;		
		for (BatchNode batchNode : batchNodes) {
			size += batchNode.batchNamespaces.size();
		}
		completedSize = size;		
	}	
}
