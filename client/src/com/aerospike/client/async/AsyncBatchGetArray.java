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
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.command.BatchItem;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchGetArray extends AsyncMultiCommand {
	private final BatchNode.BatchNamespace batchNamespace;
	private final Policy policy;
	private final HashMap<Key,BatchItem> keyMap;
	private final Record[] records;
	private final int readAttr;
	
	public AsyncBatchGetArray(
		AsyncMultiExecutor parent,
		AsyncCluster cluster,
		AsyncNode node,
		BatchNode.BatchNamespace batchNamespace,
		Policy policy,
		HashMap<Key,BatchItem> keyMap,
		HashSet<String> binNames,
		Record[] records,
		int readAttr
	) {
		super(parent, cluster, node, false, binNames);
		this.batchNamespace = batchNamespace;
		this.policy = policy;
		this.keyMap = keyMap;
		this.records = records;
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
		BatchItem item = keyMap.get(key);
		
		if (item != null) {				
			if (resultCode == 0) {
				int index = item.getIndex();
				records[index] = parseRecordWithDuplicates();
			}
		}
		else {
			if (Log.debugEnabled()) {
				Log.debug("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest));
			}
		}
	}
}
