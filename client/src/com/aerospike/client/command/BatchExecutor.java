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
package com.aerospike.client.command;

import java.util.HashSet;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.policy.BatchPolicy;

public final class BatchExecutor {
	
	public static void execute(
		Cluster cluster,
		BatchPolicy policy,
		Key[] keys,
		boolean[] existsArray,
		Record[] records,
		HashSet<String> binNames,
		int readAttr
	) throws AerospikeException {
		
		if (keys.length == 0) {
			return;
		}
		
		List<BatchNode> batchNodes = BatchNode.generateList(cluster, keys);

		if (policy.maxConcurrentThreads == 1) {
			// Run batch requests sequentially in same thread.
			for (BatchNode batchNode : batchNodes) {
				for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
					if (records != null) {
						BatchCommandGet command = new BatchCommandGet(batchNode.node, batchNamespace, policy, keys, binNames, records, readAttr);
						command.execute();
					}
					else {
						BatchCommandExists command = new BatchCommandExists(batchNode.node, batchNamespace, policy, keys, existsArray);
						command.execute();
					}
				}
			}
		}
		else {
			// Run batch requests in parallel in separate threads.
			Executor executor = new Executor(cluster, batchNodes.size() * 2);

			// Initialize threads.  There may be multiple threads for a single node because the
			// wire protocol only allows one namespace per command.  Multiple namespaces 
			// require multiple threads per node.
			for (BatchNode batchNode : batchNodes) {
				for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
					if (records != null) {
						MultiCommand command = new BatchCommandGet(batchNode.node, batchNamespace, policy, keys, binNames, records, readAttr);
						executor.addCommand(command);
					}
					else {
						MultiCommand command = new BatchCommandExists(batchNode.node, batchNamespace, policy, keys, existsArray);
						executor.addCommand(command);
					}
				}
			}
			executor.execute(policy.maxConcurrentThreads);
		}		
	}
}
