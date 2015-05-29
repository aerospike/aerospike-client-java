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

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.listener.BatchRecordListener;
import com.aerospike.client.policy.BatchPolicy;

public final class AsyncBatchGetVarBinsArrayExecutor extends AsyncMultiExecutor {
	private final BatchRecordListener listener;
	private final List<BatchRecord> records;

	public AsyncBatchGetVarBinsArrayExecutor(
		AsyncCluster cluster,
		BatchPolicy policy, 
		BatchRecordListener listener,
		List<BatchRecord> records
	) {
		this.listener = listener;
		this.records = records;
		
		// Create commands.
		List<BatchNode> batchNodes = BatchNode.generateList(cluster, policy, records);
		AsyncMultiCommand[] tasks = new AsyncMultiCommand[batchNodes.size()];
		int count = 0;

		for (BatchNode batchNode : batchNodes) {
			if (! batchNode.node.hasBatchIndex) {
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Requested command requires a server that supports new batch index protocol.");
			}
			tasks[count++] = new AsyncBatchGetVarBinsArray(this, cluster, batchNode, policy, records);
		}
		// Dispatch commands to nodes.
		execute(tasks, policy.maxConcurrentThreads);
	}
	
	protected void onSuccess() {
		listener.onSuccess(records);
	}
	
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}		
}
