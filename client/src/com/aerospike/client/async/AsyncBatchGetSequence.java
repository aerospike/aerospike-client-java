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
import com.aerospike.client.listener.RecordSequenceListener;

public final class AsyncBatchGetSequence extends AsyncMultiCommand {
	private final RecordSequenceListener listener;
	
	public AsyncBatchGetSequence(
		AsyncMultiExecutor parent,
		AsyncCluster cluster,
		AsyncNode node,
		HashSet<String> binNames,
		RecordSequenceListener listener
	) {
		super(parent, cluster, node, false, binNames);
		this.listener = listener;
	}
		
	@Override
	protected void parseRow(Key key) throws AerospikeException {		
		Record record = parseRecordWithDuplicates();
		listener.onRecord(key, record);
	}
}
