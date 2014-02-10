/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
