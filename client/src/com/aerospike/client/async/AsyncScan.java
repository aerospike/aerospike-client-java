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

public final class AsyncScan extends AsyncMultiCommand {
	private final RecordSequenceListener listener;
	
	public AsyncScan(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, RecordSequenceListener listener) {
		super(parent, cluster, node);
		this.listener = listener;
	}
		
	@Override
	protected void parseRow(Key key) throws AerospikeException {		
		Record record = parseRecord();
		listener.onRecord(key, record);
	}
}
