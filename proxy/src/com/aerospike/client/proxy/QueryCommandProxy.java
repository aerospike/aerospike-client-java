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

import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcConversions;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.client.query.Statement;
import com.aerospike.proxy.client.Kvs;
import com.aerospike.proxy.client.QueryGrpc;

/**
 * Implements asynchronous query for the proxy.
 */
public class QueryCommandProxy extends ScanQueryBaseCommandProxy {
	private final Statement statement;
	private final PartitionFilter partitionFilter;
	private final long taskId;

	public QueryCommandProxy(
		GrpcCallExecutor executor,
		RecordSequenceListener listener, QueryPolicy queryPolicy,
		Statement statement,
		long taskId,
		PartitionFilter partitionFilter,
		PartitionTracker partitionTracker
	) {
		super(false, QueryGrpc.getQueryStreamingMethod(), executor, queryPolicy,
			listener, partitionTracker);
		this.statement = statement;
		this.taskId = taskId;

		// gRPC query policy does not have the deprecated maxRecords field.
		// Set the max records in the statement from query policy.
		// noinspection deprecation
		this.statement.setMaxRecords(statement.getMaxRecords() > 0 ?
			statement.getMaxRecords() : queryPolicy.maxRecords);

		this.partitionFilter = partitionFilter;
	}

	@Override
	protected Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
		// Set the query parameters in the Aerospike request payload.
		Kvs.AerospikeRequestPayload.Builder builder = Kvs.AerospikeRequestPayload.newBuilder();
		Kvs.QueryRequest.Builder queryRequestBuilder = Kvs.QueryRequest.newBuilder();

		queryRequestBuilder.setQueryPolicy(GrpcConversions.toGrpc((QueryPolicy)policy));
		if (partitionFilter != null) {
			queryRequestBuilder.setPartitionFilter(GrpcConversions.toGrpc(partitionFilter));
		}
		queryRequestBuilder.setStatement(GrpcConversions.toGrpc(statement, taskId));
		builder.setQueryRequest(queryRequestBuilder.build());

		return builder;
	}
}
