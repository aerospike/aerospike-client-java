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
import com.aerospike.client.Record;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchGetSequence extends AsyncMultiCommand {
	private final BatchNode.BatchNamespace batchNamespace;
	private final Policy policy;
	private final RecordSequenceListener listener;
	private final int readAttr;
	
	public AsyncBatchGetSequence(
		AsyncMultiExecutor parent,
		AsyncCluster cluster,
		AsyncNode node,
		BatchNode.BatchNamespace batchNamespace,
		Policy policy,
		HashSet<String> binNames,
		RecordSequenceListener listener,
		int readAttr
	) {
		super(parent, cluster, node, false, binNames);
		this.batchNamespace = batchNamespace;
		this.policy = policy;
		this.listener = listener;
		this.readAttr = readAttr;
	}
		
	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setBatchGet(batchNamespace, binNames, readAttr);
	}

	@Override
	protected void parseRow(Key key) throws AerospikeException {		
		Record record = parseRecordWithDuplicates();
		listener.onRecord(key, record);
	}
}
