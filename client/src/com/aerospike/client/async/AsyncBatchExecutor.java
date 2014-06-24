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

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchNode;

public abstract class AsyncBatchExecutor extends AsyncMultiExecutor {
	protected final Key[] keys;
	protected final List<BatchNode> batchNodes;

	public AsyncBatchExecutor(Cluster cluster, Key[] keys) throws AerospikeException {
		this.keys = keys;	
		this.batchNodes = BatchNode.generateList(cluster, keys);
		
		// Count number of asynchronous commands needed.
		int size = 0;		
		for (BatchNode batchNode : batchNodes) {
			size += batchNode.batchNamespaces.size();
		}
		completedSize = size;		
	}	
}
