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
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.ScanPolicy;

public final class AsyncScanExecutor extends AsyncMultiExecutor {
	private final RecordSequenceListener listener;

	public AsyncScanExecutor(
		AsyncCluster cluster,
		ScanPolicy policy,
		RecordSequenceListener listener,
		String namespace,
		String setName,
		String[] binNames
	) throws AerospikeException {
		this.listener = listener;

		Node[] nodes = cluster.getNodes();
		completedSize = nodes.length;

		for (Node node : nodes) {			
			AsyncScan async = new AsyncScan(this, cluster, (AsyncNode)node, policy, listener, namespace, setName, binNames);
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
