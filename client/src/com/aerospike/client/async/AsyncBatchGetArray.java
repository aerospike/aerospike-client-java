/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.async;

import java.util.Arrays;
import java.util.HashSet;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchGetArray extends AsyncMultiCommand {
	private final BatchNode.BatchNamespace batch;
	private final Policy policy;
	private final Key[] keys;
	private final Record[] records;
	private final int readAttr;
	private int index;
	
	public AsyncBatchGetArray(
		AsyncMultiExecutor parent,
		AsyncCluster cluster,
		AsyncNode node,
		BatchNode.BatchNamespace batch,
		Policy policy,
		Key[] keys,
		HashSet<String> binNames,
		Record[] records,
		int readAttr
	) {
		super(parent, cluster, node, false, binNames);
		this.batch = batch;
		this.policy = policy;
		this.keys = keys;
		this.records = records;
		this.readAttr = readAttr;
	}
		
	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setBatchGet(keys, batch, binNames, readAttr);
	}

	@Override
	protected void parseRow(Key key) throws AerospikeException {
		int offset = batch.offsets[index++];

		if (Arrays.equals(key.digest, keys[offset].digest)) {			
			if (resultCode == 0) {
				records[offset] = parseRecordBatch();
			}
		}
		else {
			if (Log.warnEnabled()) {
				Log.warn("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest) + ',' + index + ',' + offset);
			}
		}
	}
}
