/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.client.command;

import java.util.List;

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
		String[] binNames,
		int readAttr
	) {	
		if (keys.length == 0) {
			return;
		}
		
		List<BatchNode> batchNodes = BatchNode.generateList(cluster, policy, keys);

		if (policy.maxConcurrentThreads == 1 || batchNodes.size() <= 1) {
			// Run batch requests sequentially in same thread.
			for (BatchNode batchNode : batchNodes) {
				if (batchNode.node.useNewBatch(policy)) {
					// New batch
					if (records != null) {
						MultiCommand command = new Batch.GetArrayCommand(batchNode, policy, keys, binNames, records, readAttr);
						command.execute(cluster, policy, null, batchNode.node, true);
					}
					else {
						MultiCommand command = new Batch.ExistsArrayCommand(batchNode, policy, keys, existsArray);
						command.execute(cluster, policy, null, batchNode.node, true);
					}
				}
				else {
					// Old batch only allows one namespace per call.
					batchNode.splitByNamespace(keys);
					
					for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
						if (records != null) {
							MultiCommand command = new Batch.GetArrayDirect(batchNamespace, policy, keys, binNames, records, readAttr);
							command.execute(cluster, policy, null, batchNode.node, true);
						}
						else {
							MultiCommand command = new Batch.ExistsArrayDirect(batchNamespace, policy, keys, existsArray);
							command.execute(cluster, policy, null, batchNode.node, true);
						}
					}
				}
			}
		}
		else {
			// Run batch requests in parallel in separate threads.
			//			
			// Multiple threads write to the record/exists array, so one might think that
			// volatile or memory barriers are needed on the write threads and this read thread.
			// This should not be necessary here because it happens in Executor which does a 
			// volatile write (completedCount.incrementAndGet()) at the end of write threads
			// and a synchronized waitTillComplete() in this thread.
			Executor executor = new Executor(cluster, policy, batchNodes.size() * 2);

			// Initialize threads.  
			for (BatchNode batchNode : batchNodes) {
				if (batchNode.node.useNewBatch(policy)) {
					// New batch
					if (records != null) {
						MultiCommand command = new Batch.GetArrayCommand(batchNode, policy, keys, binNames, records, readAttr);
						executor.addCommand(batchNode.node, command);
					}
					else {
						MultiCommand command = new Batch.ExistsArrayCommand(batchNode, policy, keys, existsArray);
						executor.addCommand(batchNode.node, command);
					}
				}
				else {
					// There may be multiple threads for a single node because the
					// wire protocol only allows one namespace per command.  Multiple namespaces 
					// require multiple threads per node.
					batchNode.splitByNamespace(keys);

					for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
						if (records != null) {
							MultiCommand command = new Batch.GetArrayDirect(batchNamespace, policy, keys, binNames, records, readAttr);
							executor.addCommand(batchNode.node, command);
						}
						else {
							MultiCommand command = new Batch.ExistsArrayDirect(batchNamespace, policy, keys, existsArray);
							executor.addCommand(batchNode.node, command);
						}
					}
				}
			}
			executor.execute(policy.maxConcurrentThreads);
		}		
	}
}
