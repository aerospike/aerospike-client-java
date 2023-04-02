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

import javax.annotation.Nullable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Command;
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
public class QueryCommandProxy extends CommandProxy {
	private final Statement statement;

	private final RecordSequenceListener listener;
	private final PartitionFilter partitionFilter;
	private final PartitionTracker partitionTracker;
	private final PartitionTracker.NodePartitions dummyNodePartitions;

	public QueryCommandProxy(GrpcCallExecutor executor,
							 RecordSequenceListener listener, QueryPolicy queryPolicy,
							 Statement statement,
							 @Nullable
							 PartitionFilter partitionFilter,
							 @Nullable
							 PartitionTracker partitionTracker
	) {
		super(QueryGrpc.getQueryStreamingMethod(), executor, queryPolicy);
		this.statement = statement;

		// gRPC query policy does not have the deprecated maxRecords field.
		// Set the max records in the statement from query policy.
		// noinspection deprecation
		this.statement.setMaxRecords(statement.getMaxRecords() > 0 ?
			statement.getMaxRecords() :
			queryPolicy.maxRecords);

		this.listener = listener;
		this.partitionFilter = partitionFilter;
		this.partitionTracker = partitionTracker;
		this.dummyNodePartitions = new PartitionTracker.NodePartitions(null,
			Node.PARTITIONS);
	}

	@Override
	protected void writeCommand(Command command) {
		// Nothing to do since there is no Aerospike payload.
	}

	@Override
	protected void parseResult(Parser parser, boolean isLast) {
		RecordProxy recordProxy = parseRecordResult(parser, false, true,
			true);

		if (recordProxy.resultCode == ResultCode.OK && recordProxy.key == null) {
			// This is the end of query marker record.
			listener.onSuccess();
			return;
		}

		listener.onRecord(recordProxy.key, recordProxy.record);

		if (partitionTracker != null) {
			partitionTracker.setLast(dummyNodePartitions, recordProxy.key,
				recordProxy.bVal.val);
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
		if (partitionFilter != null) {
			queryRequestBuilder.setPartitionFilter(GrpcConversions.toGrpc(partitionFilter));
		}
		queryRequestBuilder.setStatement(GrpcConversions.toGrpc(statement));
		builder.setQueryRequest(queryRequestBuilder.build());

		return builder;
	}
}
