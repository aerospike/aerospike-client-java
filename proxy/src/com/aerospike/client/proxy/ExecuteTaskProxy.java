/*
 * Copyright 2012-2023 Aerospike, Inc.
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

package com.aerospike.client.proxy;

import java.util.concurrent.CompletableFuture;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.task.ExecuteTask;

public class ExecuteTaskProxy extends ExecuteTask {
	private final long taskId;
	private final boolean scan;
	private final GrpcCallExecutor callExecutor;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public ExecuteTaskProxy(GrpcCallExecutor executor, long taskId, boolean isScan) {
		super(taskId, isScan);
		this.callExecutor = executor;
		this.taskId = taskId;
		this.scan = isScan;
	}

	/**
	 * Return task id.
	 */
	public long getTaskId() {
		return taskId;
	}

	/**
	 * Query all nodes for task completion status.
	 */
	@Override
	public int queryStatus() throws AerospikeException {
		CompletableFuture<Integer> future = new CompletableFuture<>();
		ExecuteTaskStatusCommandProxy command = new ExecuteTaskStatusCommandProxy(callExecutor,
			new WritePolicy(), taskId, scan, future);
		command.execute();
		return AerospikeClientProxy.getFuture(future);
	}
}