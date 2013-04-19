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
import com.aerospike.client.command.Buffer;

public final class AsyncBatchGetArray extends AsyncMultiCommand {
	private final HashMap<Key,BatchItem> keyMap;
	private final Record[] records;
	
	public AsyncBatchGetArray(
		AsyncMultiExecutor parent,
		AsyncCluster cluster,
		AsyncNode node,
		HashMap<Key,BatchItem> keyMap,
		HashSet<String> binNames,
		Record[] records
	) {
		super(parent, cluster, node, binNames);
		this.keyMap = keyMap;
		this.records = records;
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
