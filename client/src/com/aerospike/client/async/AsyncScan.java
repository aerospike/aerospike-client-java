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
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;

public final class AsyncScan extends AsyncMultiCommand {
	private final ScanPolicy policy;
	private final RecordSequenceListener listener;
	private final String namespace;
	private final String setName;
	private final String[] binNames;
	
	public AsyncScan(
		AsyncMultiExecutor parent,
		AsyncCluster cluster,
		AsyncNode node,
		ScanPolicy policy,
		RecordSequenceListener listener,
		String namespace,
		String setName,
		String[] binNames
	) {
		super(parent, cluster, node, true);
		this.policy = policy;
		this.listener = listener;
		this.namespace = namespace;
		this.setName = setName;
		this.binNames = binNames;
	}
		
	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setScan(policy, namespace, setName, binNames);
	}

	@Override
	protected void parseRow(Key key) throws AerospikeException {		
		Record record = parseRecord();
		listener.onRecord(key, record);
	}
}
