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
public class BackgroundExecuteCommandProxy extends CommandProxy {
	private final Statement statement;

	private final CompletableFuture<Void> future;

	/**
	 * @param executor    the gRPC call executor
	 * @param writePolicy the query write policy
	 * @param statement   the query statement
	 * @param future      future to signal success or failure of starting the
	 *                    background query
	 */
	public BackgroundExecuteCommandProxy(GrpcCallExecutor executor,
										 WritePolicy writePolicy,
										 Statement statement,
										 CompletableFuture<Void> future) {
		super(QueryGrpc.getBackgroundExecuteStreamingMethod(), executor, writePolicy);
		this.statement = statement;
		this.future = future;
	}

	@Override
	protected void writeCommand(Command command) {
		// Nothing to do since there is no Aerospike payload.
	}

	@Override
	protected void parseResult(Parser parser, boolean isLast) {
		RecordProxy recordProxy = parseRecordResult(parser, false, true,
			false);

		// Only on response is expected.
		if (recordProxy.resultCode != ResultCode.OK) {
			throw new AerospikeException(recordProxy.resultCode);
		}

		future.complete(null);
	}

	@Override
	protected void onFailure(AerospikeException ae) {
		future.completeExceptionally(ae);
	}

	@Override
	protected Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
		// Set the query parameters in the Aerospike request payload.
		Kvs.AerospikeRequestPayload.Builder builder = Kvs.AerospikeRequestPayload.newBuilder();
		Kvs.BackgroundExecuteRequest.Builder queryRequestBuilder =
			Kvs.BackgroundExecuteRequest.newBuilder();

		queryRequestBuilder.setWritePolicy(GrpcConversions.toGrpc((WritePolicy)policy));
		queryRequestBuilder.setStatement(GrpcConversions.toGrpc(statement));
		builder.setBackgroundExecuteRequest(queryRequestBuilder.build());

		return builder;
	}
}
