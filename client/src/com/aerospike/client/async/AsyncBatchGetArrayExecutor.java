/*
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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

import java.util.HashSet;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.policy.BatchPolicy;

public final class AsyncBatchGetArrayExecutor extends AsyncBatchExecutor {
	private final RecordArrayListener listener;
	private final Record[] recordArray;

	public AsyncBatchGetArrayExecutor(
		AsyncCluster cluster,
		BatchPolicy policy, 
		RecordArrayListener listener,
		Key[] keys,
		HashSet<String> binNames,
		int readAttr
	) throws AerospikeException {
		super(cluster, policy, keys);
		this.recordArray = new Record[keys.length];
		this.listener = listener;
		
		// Create commands.
		AsyncBatchGetArray[] tasks = new AsyncBatchGetArray[super.taskSize];
		int count = 0;

		for (BatchNode batchNode : batchNodes) {			
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {				
				tasks[count++] = new AsyncBatchGetArray(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keys, binNames, recordArray, readAttr);
			}
		}
		// Dispatch commands to nodes.
		execute(tasks, policy.maxConcurrentThreads);
	}
	
	protected void onSuccess() {
		listener.onSuccess(keys, recordArray);
	}
	
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}		
}
