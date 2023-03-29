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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcConversions;
import com.aerospike.client.query.Statement;
import com.aerospike.proxy.client.Kvs;
import com.aerospike.proxy.client.QueryGrpc;

public class QueryCommandProxy extends CommandProxy {

	private final Statement statement;

	private final RecordSequenceListener listener;

	public QueryCommandProxy(GrpcCallExecutor executor,
							 QueryPolicy queryPolicy,
							 Statement statement,
							 RecordSequenceListener listener
	) {
		super(QueryGrpc.getQueryStreamingMethod(), executor, queryPolicy);
		this.statement = statement;
		this.listener = listener;
	}

	@Override
	protected void writeCommand(Command command) {
		// Nothing to do since there is no Aerospike payload.
	}

	@Override
	protected void parseResult(Parser parser) {
		ProxyRecord proxyRecord = parseRecordResult(parser, false, true,
			false);

		if (proxyRecord.resultCode == ResultCode.OK && proxyRecord.key == null) {
			// This is the end of query marker record.
			listener.onSuccess();
			return;
		}

		try {
			listener.onRecord(proxyRecord.key, proxyRecord.record);
		}
		catch (Throwable t) {
			// Exception thrown from the server.
			// TODO: sent a request to the proxy server to abort the scan.
			logOnSuccessError(t);
		}
	}

	@Override
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}

	@Override
	protected Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
		// Set the query parameters in the Aerospike request payload.
		Kvs.AerospikeRequestPayload.Builder builder = Kvs.AerospikeRequestPayload.newBuilder();
		Kvs.QueryRequest.Builder queryRequestBuilder =
			Kvs.QueryRequest.newBuilder();

		queryRequestBuilder.setQueryPolicy(GrpcConversions.toGrpc((QueryPolicy)policy));

		queryRequestBuilder.setStatement(GrpcConversions.toGrpc(statement));
		builder.setQueryRequest(queryRequestBuilder.build());
		return builder;
	}
}
