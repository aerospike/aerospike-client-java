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
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.proxy.client.Kvs;
import com.aerospike.proxy.client.QueryGrpc;

/**
 * Fetch status for a background task.
 */
public class ExecuteTaskStatusCommandProxy extends MultiCommandProxy {
	private final long taskId;
	private final boolean isScan;
	private final CompletableFuture<Integer> future;
	private int status;

	public ExecuteTaskStatusCommandProxy(
		GrpcCallExecutor executor,
		WritePolicy writePolicy,
		long taskId,
		boolean isScan,
		CompletableFuture<Integer> future
	) {
		super(QueryGrpc.getBackgroundTaskStatusStreamingMethod(), executor, writePolicy);
		this.taskId = taskId;
		this.isScan = isScan;
		this.future = future;
	}

	@Override
	protected void writeCommand(Command command) {
		// Nothing to do since there is no Aerospike payload.
	}

	@Override
	protected void parseResult(Parser parser) {
		RecordProxy recordProxy = parseRecordResult(parser, false, true, false);

		// Only on response is expected.
		if (recordProxy.resultCode != ResultCode.OK) {
			throw new AerospikeException(recordProxy.resultCode);
		}

		// Status has been set in onResponse
		future.complete(status);
	}

	@Override
	void onResponse(Kvs.AerospikeResponsePayload response) {
		// Set the value but do not report the result until the resultcode
		// is computed in parseResult
		if (!response.hasField(response.getDescriptorForType().findFieldByName("backgroundTaskStatus"))) {
			throw new AerospikeException.Parse("missing task status field");
		}
		status = response.getBackgroundTaskStatusValue();
		super.onResponse(response);
	}

	@Override
	protected void onFailure(AerospikeException ae) {
		future.completeExceptionally(ae);
	}

	@Override
	protected Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
		// Set the query parameters in the Aerospike request payload.
		Kvs.AerospikeRequestPayload.Builder builder = Kvs.AerospikeRequestPayload.newBuilder();
		Kvs.BackgroundTaskStatusRequest.Builder statusRequestBuilder = Kvs.BackgroundTaskStatusRequest.newBuilder();
		statusRequestBuilder.setTaskId(taskId);
		statusRequestBuilder.setIsScan(isScan);

		builder.setBackgroundTaskStatusRequest(statusRequestBuilder.build());
		return builder;
	}
}
