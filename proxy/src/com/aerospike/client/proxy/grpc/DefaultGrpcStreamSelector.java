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
package com.aerospike.client.proxy.grpc;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.aerospike.proxy.client.Kvs;
import com.aerospike.proxy.client.QueryGrpc;
import com.aerospike.proxy.client.ScanGrpc;

/**
 * A default gRPC stream selector which selects a free stream.
 */
public class DefaultGrpcStreamSelector implements GrpcStreamSelector {
	private final int maxConcurrentStreamsPerChannel;
	private final int maxConcurrentRequestsPerStream;
	private final int totalRequestsPerStream;

	public DefaultGrpcStreamSelector(int maxConcurrentStreamsPerChannel, int maxConcurrentRequestsPerStream, int totalRequestsPerStream) {
		this.maxConcurrentStreamsPerChannel = maxConcurrentStreamsPerChannel;
		this.maxConcurrentRequestsPerStream = maxConcurrentRequestsPerStream;
		this.totalRequestsPerStream = totalRequestsPerStream;
	}

	@Override
	public SelectedStream select(List<GrpcStream> streams, GrpcStreamingCall call) {
		final String fullMethodName =
			call.getStreamingMethodDescriptor().getFullMethodName();

		// Always use a dedicated new stream for a scan and a long query.
		if (isScan(call) || isLongQuery(call)) {
			return new SelectedStream(1, 1);
		}

		// Sort by stream id. Leave original list as it is.
		List<GrpcStream> filteredStreams = streams.stream()
			.filter(grpcStream ->
				grpcStream.getMethodDescriptor().getFullMethodName()
					.equals(fullMethodName)
			)
			.sorted(Comparator.comparingInt(GrpcStream::getId))
			.collect(Collectors.toList());

		// Select first stream with less than max concurrent requests.
		for (GrpcStream stream : filteredStreams) {
			if (stream.getOngoingRequests() < stream.getMaxConcurrentRequests()) {
				return new SelectedStream(stream);
			}
		}

		if (streams.size() < maxConcurrentStreamsPerChannel) {
			// Create new stream.
			return new SelectedStream(maxConcurrentRequestsPerStream, totalRequestsPerStream);
		}

		// TODO What is the probability of this occurring? Should some streams
		//  in a channel be reserved for rarely used API's?
		if (filteredStreams.isEmpty()) {
			// Create new stream.
			return new SelectedStream(maxConcurrentRequestsPerStream, totalRequestsPerStream);
		}

		// Select stream with lowest percent of total requests executed.
		GrpcStream selected = filteredStreams.get(0);
		for (GrpcStream stream : filteredStreams) {
			float executedPercent =
				(float)stream.getExecutedRequests() / stream.getTotalRequestsToExecute();
			float selectedPercent =
				(float)selected.getExecutedRequests() / stream.getTotalRequestsToExecute();
			if (executedPercent < selectedPercent) {
				selected = stream;
			}
		}
		return new SelectedStream(selected);
	}

	private boolean isScan(GrpcStreamingCall call) {
		String fullMethodName =
			call.getStreamingMethodDescriptor().getFullMethodName();
		String scanFullMethodName =
			ScanGrpc.getScanMethod().getFullMethodName();
		String scanStreamingFullMethodName =
			ScanGrpc.getScanStreamingMethod().getFullMethodName();
		return scanFullMethodName.equals(fullMethodName) ||
			scanStreamingFullMethodName.equals(fullMethodName);
	}

	private boolean isLongQuery(GrpcStreamingCall call) {
		String fullMethodName =
			call.getStreamingMethodDescriptor().getFullMethodName();
		String queryFullMethodName =
			QueryGrpc.getQueryMethod().getFullMethodName();
		String queryStreamingFullMethodName =
			QueryGrpc.getQueryStreamingMethod().getFullMethodName();

		if (!queryFullMethodName.equals(fullMethodName) &&
			!queryStreamingFullMethodName.equals(fullMethodName)) {
			return false;  // Not a query request.
		}

		Kvs.QueryRequest queryRequest = call.getRequestBuilder().getQueryRequest();
		if (queryRequest.getBackground()) {
			return false; // Background queries send back a single response.
		}

		if (queryRequest.getStatement().getMaxRecords() < 10) {
			return false; // Records returned in responses is small.
		}

		if (!queryRequest.getStatement().getFunctionName().isEmpty()) {
			return false; // Is an aggregation statement.
		}

		if (queryRequest.hasQueryPolicy() && queryRequest.getQueryPolicy().getShortQuery()) {
			return false; // Is a short query.
		}

		return true;
	}
}
