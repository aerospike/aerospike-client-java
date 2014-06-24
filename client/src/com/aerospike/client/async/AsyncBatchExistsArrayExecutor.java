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

import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.command.BatchItem;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchExistsArrayExecutor extends AsyncBatchExecutor {
	private final ExistsArrayListener listener;
	private final boolean[] existsArray;

	public AsyncBatchExistsArrayExecutor(
		AsyncCluster cluster,
		Policy policy, 
		Key[] keys,
		ExistsArrayListener listener
	) throws AerospikeException {
		super(cluster, keys);
		this.existsArray = new boolean[keys.length];
		this.listener = listener;
		
		if (policy == null) {
			policy = new Policy();
		}
		
		HashMap<Key,BatchItem> keyMap = BatchItem.generateMap(keys);
		
		// Dispatch asynchronous commands to nodes.
		for (BatchNode batchNode : batchNodes) {			
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
				AsyncBatchExistsArray async = new AsyncBatchExistsArray(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keyMap, existsArray);
				async.execute();
			}
		}
	}
	
	protected void onSuccess() {
		listener.onSuccess(keys, existsArray);
	}
	
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}		
}
