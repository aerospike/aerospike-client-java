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
import com.aerospike.client.Log;
import com.aerospike.client.command.BatchItem;
import com.aerospike.client.command.Buffer;

public final class AsyncBatchExistsArray extends AsyncMultiCommand {
	private final HashMap<Key,BatchItem> keyMap;
	private final boolean[] existsArray;
	
	public AsyncBatchExistsArray(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, HashMap<Key,BatchItem> keyMap, boolean[] existsArray) {
		super(parent, cluster, node, false);
		this.keyMap = keyMap;
		this.existsArray = existsArray;
	}
		
	@Override
	protected void parseRow(Key key) throws AerospikeException {		
		if (opCount > 0) {
			throw new AerospikeException.Parse("Received bins that were not requested!");
		}			

		BatchItem item = keyMap.get(key);
		
		if (item != null) {
			int index = item.getIndex();
			existsArray[index] = resultCode == 0;
		}
		else {
			if (Log.debugEnabled()) {
				Log.debug("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest));
			}
		}
	}
}
