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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.command.BatchItem;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchExistsArray extends AsyncMultiCommand {
	private final BatchNode.BatchNamespace batchNamespace;
	private final Policy policy;
	private final HashMap<Key,BatchItem> keyMap;
	private final boolean[] existsArray;
	
	public AsyncBatchExistsArray(
		AsyncMultiExecutor parent,
		AsyncCluster cluster,
		AsyncNode node,
		BatchNode.BatchNamespace batchNamespace,
		Policy policy,
		HashMap<Key,BatchItem> keyMap,
		boolean[] existsArray
	) {
		super(parent, cluster, node, false);
		this.batchNamespace = batchNamespace;
		this.policy = policy;
		this.keyMap = keyMap;
		this.existsArray = existsArray;
	}
		
	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setBatchExists(batchNamespace);
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
