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

import java.util.HashSet;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchGetSequenceExecutor extends AsyncBatchExecutor {
	private final RecordSequenceListener listener;

	public AsyncBatchGetSequenceExecutor(
		AsyncCluster cluster,
		Policy policy, 
		RecordSequenceListener listener,
		Key[] keys,
		HashSet<String> binNames,
		int readAttr
	) throws AerospikeException {
		super(cluster, keys);
		this.listener = listener;
		
		if (policy == null) {
			policy = new Policy();
		}

		// Dispatch asynchronous commands to nodes.
		for (BatchNode batchNode : batchNodes) {			
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
				AsyncBatchGetSequence async = new AsyncBatchGetSequence(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, binNames, listener, readAttr);
				async.execute();
			}
		}
	}
	
	@Override
	protected void onSuccess() {
		listener.onSuccess();
	}
	
	@Override
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}		
}
