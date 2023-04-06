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
import com.aerospike.client.proxy.grpc.GrpcConversions;
import com.aerospike.client.query.Statement;
import com.aerospike.proxy.client.Kvs;
import com.aerospike.proxy.client.QueryGrpc;

/**
 * Implements asynchronous background query execute for the proxy.
 */
public class BackgroundExecuteCommandProxy extends MultiCommandProxy {
	private final Statement statement;
	private final long taskId;
	private final CompletableFuture<Void> future;

	public BackgroundExecuteCommandProxy(
		GrpcCallExecutor executor,
		WritePolicy writePolicy,
		Statement statement,
		long taskId,
		CompletableFuture<Void> future
	) {
		super(QueryGrpc.getBackgroundExecuteStreamingMethod(), executor, writePolicy);
		this.statement = statement;
		this.taskId = taskId;
		this.future = future;
	}

	@Override
	void writeCommand(Command command) {
		// Nothing to do since there is no Aerospike payload.
	}

	@Override
	void parseResult(Parser parser) {
		RecordProxy recordProxy = parseRecordResult(parser, false, true, false);

		// Only on response is expected.
		if (recordProxy.resultCode != ResultCode.OK) {
			throw new AerospikeException(recordProxy.resultCode);
		}

		future.complete(null);
	}

	@Override
	void onFailure(AerospikeException ae) {
		future.completeExceptionally(ae);
	}

	@Override
	protected Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
		// Set the query parameters in the Aerospike request payload.
		Kvs.AerospikeRequestPayload.Builder builder = Kvs.AerospikeRequestPayload.newBuilder();
		Kvs.BackgroundExecuteRequest.Builder queryRequestBuilder =
			Kvs.BackgroundExecuteRequest.newBuilder();

		queryRequestBuilder.setWritePolicy(GrpcConversions.toGrpc((WritePolicy)policy));
		queryRequestBuilder.setStatement(GrpcConversions.toGrpc(statement, taskId));
		builder.setBackgroundExecuteRequest(queryRequestBuilder.build());

		return builder;
	}
}
