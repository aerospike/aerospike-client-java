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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * A default gRPC stream selector which selects channel by the low and high
 * water mark.
 */
public class DefaultGrpcChannelSelector implements GrpcChannelSelector {
	private final int requestsLowWaterMark;
	private final int requestsHighWaterMark;
	private final Random random = new Random();

	public DefaultGrpcChannelSelector(int requestsLowWaterMark, int requestsHighWaterMark) {
		this.requestsLowWaterMark = requestsLowWaterMark;
		this.requestsHighWaterMark = requestsHighWaterMark;
	}

	@Override
	public GrpcChannelExecutor select(List<GrpcChannelExecutor> channels, GrpcStreamingCall call) {
		// Sort by channel id. Leave original list as it is.
		channels = new ArrayList<>(channels);
		channels.sort(Comparator.comparingLong(GrpcChannelExecutor::getId));

		// Select the first channel below the low watermark.
		for (GrpcChannelExecutor channel : channels) {
			if (channel.getOngoingRequests() < requestsLowWaterMark) {
				return channel;
			}
		}

		// FIXME: it might be the case that the channel has opened
		//  maxConcurrentStreams but none of them are for this grpcCall. This
		//  also needs to be checked when selecting the channel.

		// All channels are above the low watermark, select the first channel
		// below the high watermark.
		for (GrpcChannelExecutor channel : channels) {
			if (channel.getOngoingRequests() < requestsHighWaterMark) {
				return channel;
			}
		}

		// TODO: maybe we should use in-flight bytes, number of streams, or
		//  some other parameter to select the channel.
		// All channels are above the high water mark, select random channel.
		return channels.get(random.nextInt(channels.size()));
	}
}
