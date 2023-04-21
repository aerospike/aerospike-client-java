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

import io.grpc.MethodDescriptor;

/**
 * A default gRPC stream selector which selects a free stream.
 */
public class DefaultGrpcStreamSelector implements GrpcStreamSelector {
	private final int maxConcurrentStreamsPerChannel;
	private final int maxConcurrentRequestsPerStream;

	public DefaultGrpcStreamSelector(int maxConcurrentStreamsPerChannel, int maxConcurrentRequestsPerStream) {
		this.maxConcurrentStreamsPerChannel = maxConcurrentStreamsPerChannel;
		this.maxConcurrentRequestsPerStream = maxConcurrentRequestsPerStream;
	}

	@Override
	public GrpcStream select(
		List<GrpcStream> streams,
		MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor
	) {
		// Sort by stream id. Leave original list as it is.
		List<GrpcStream> filteredStreams = streams.stream()
				.filter(grpcStream ->
						grpcStream.getMethodDescriptor().getFullMethodName()
								.equals(methodDescriptor.getFullMethodName())
				)
				.sorted(Comparator.comparingInt(GrpcStream::getId))
				.collect(Collectors.toList());

		// Select first stream with less than max concurrent requests.
		for (GrpcStream stream : filteredStreams) {
			if (stream.getOngoingRequests() < maxConcurrentRequestsPerStream) {
				return stream;
			}
		}

		if (streams.size() < maxConcurrentStreamsPerChannel) {
			return null;  // Create new stream.
		}

		// TODO What is the probability of this occurring? Should some streams
		//  in a channel be reserved for rarely used API's?
		if (filteredStreams.isEmpty()) {
			return null; // Create new stream.
		}

		// Select stream with lowest total requests.
		GrpcStream selected = filteredStreams.get(0);
		for (GrpcStream stream : filteredStreams) {
			if (stream.getTotalExecutedRequests() < selected.getTotalExecutedRequests()) {
				selected = stream;
			}
		}
		return selected;
	}
}
