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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

import io.grpc.CallOptions;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * gRPC Aerospike proxy client policy. All the knobs and configs that
 * affect the working of the gRPC proxy client are combined into this policy
 * object.
 */
public class GrpcClientPolicy {
	/**
	 * The event loops to process the gRPC HTTP/2 requests.
	 */
	public final List<EventLoop> eventLoops;

	/**
	 * The type of the eventLoops.
	 */
	public final Class<? extends Channel> channelType;

	/**
	 * Should the event loops be closed in close.
	 */
	public final boolean closeEventLoops;

	/**
	 * Maximum number of HTTP/2 channels (connections) to open to the Aerospike
	 * gRPC proxy server.
	 * <p>
	 * Generally HTTP/2 based gRPC recommends a single channel to be
	 * sufficient for most purposes. In our performance experiments we have
	 * found that any number greater than <code>8</code> yields no extra
	 * performance gains.
	 */
	public final int maxChannels;

	/**
	 * Maximum number of concurrent HTTP/2 streams to have in-flight per HTTP/2
	 * channel (connection).
	 * <p>
	 * Generally HTTP/2 servers restrict the number of concurrent HTTP/2 streams
	 * to about a <code>100</code> on a channel (connection).
	 */
	public final int maxConcurrentStreamsPerChannel;

	/**
	 * Maximum number of concurrent requests that are in-flight per streaming
	 * HTTP/2 call.
	 * <p>
	 * The Aerospike gRPC proxy server implements streaming for unary calls
	 * like Aerospike get, put, operate, etc to improve latency and throughput.
	 * <code>maxConcurrentRequestsPerStream</code> specifies the number of
	 * concurrent requests that can be sent on a single unary call based stream.
	 * <p/>
	 * <bold>NOTE</bold> This policy does not apply to queries, scans, etc.
	 */
	public final int maxConcurrentRequestsPerStream;

	/**
	 * Total number of HTTP/2 requests that are sent on a stream, after which
	 * the stream is closed.
	 * <p>
	 * The Aerospike gRPC proxy server implements streaming for unary calls
	 * like Aerospike get, put, operate, etc to improve latency and throughput.
	 * <code>totalRequestsPerStream</code> specifies the total number of
	 * requests that are sent on the stream, after which the stream is closed.
	 * <p>
	 * Requests to the Aerospike gRPC proxy server will be routed through a
	 * HTTP/2 load balancer over the public internet. HTTP/2 load balancer
	 * splits requests on a single HTTP/2 channel (connection) across the proxy
	 * servers, but it will send all requests on a HTTP/2 stream to a single
	 * gRPC Aerospike proxy server. This policy ensures that the requests are
	 * evenly load balanced across the gRPC Aerospike proxy servers.
	 * <p/>
	 * <bold>NOTE</bold> This policy does not apply to queries, scans, etc.
	 */
	public final int totalRequestsPerStream;

	/**
	 * The connection timeout in milliseconds when creating a new HTTP/2
	 * channel (connection) to a gRPC Aerospike proxy server.
	 */
	public final int connectTimeoutMillis;

	/**
	 * See {@link ClientPolicy#closeTimeout}.
	 */
	public final int closeTimeout;

	/**
	 * Strategy to select a channel for a gRPC request.
	 */

	public final GrpcChannelSelector grpcChannelSelector;

	/**
	 * Strategy to select a stream for a gRPC request.
	 */
	public final GrpcStreamSelector grpcStreamSelector;

	/**
	 * Call options.
	 */
	public final CallOptions callOptions;

	/**
	 * The TLS policy to connect to the gRPC Aerospike proxy server.
	 * <p>
	 * <bold>NOTE</bold> The channel (connection) will  be non-encrypted if
	 * this policy is <code>null</code>.
	 */
	@Nullable
	public final TlsPolicy tlsPolicy;

	/**
	 * Milliseconds to wait for termination of the channels. Should be
	 * greater than the deadlines. The implementation is best-effort, its
	 * possible termination takes more time than this.
	 */
	public final long terminationWaitMillis;

	/**
	 * Index to get the next event loop.
	 */
	private final AtomicInteger eventLoopIndex = new AtomicInteger(0);

	private GrpcClientPolicy(
		int maxChannels,
		int maxConcurrentStreamsPerChannel,
		int maxConcurrentRequestsPerStream,
		int totalRequestsPerStream,
		int connectTimeoutMillis,
		long terminationWaitMillis,
		int closeTimeout, GrpcChannelSelector grpcChannelSelector,
		GrpcStreamSelector grpcStreamSelector,
		CallOptions callOptions,
		List<EventLoop> eventLoops,
		Class<? extends Channel> channelType,
		boolean closeEventLoops,
		@Nullable TlsPolicy tlsPolicy
	) {
		this.maxChannels = maxChannels;
		this.maxConcurrentStreamsPerChannel = maxConcurrentStreamsPerChannel;
		this.maxConcurrentRequestsPerStream = maxConcurrentRequestsPerStream;
		this.totalRequestsPerStream = totalRequestsPerStream;
		this.connectTimeoutMillis = connectTimeoutMillis;
		this.terminationWaitMillis = terminationWaitMillis;
		this.closeTimeout = closeTimeout;
		this.grpcChannelSelector = grpcChannelSelector;
		this.grpcStreamSelector = grpcStreamSelector;
		this.callOptions = callOptions;
		this.eventLoops = eventLoops;
		this.channelType = channelType;
		this.closeEventLoops = closeEventLoops;
		this.tlsPolicy = tlsPolicy;
	}

	public static Builder newBuilder(
		@Nullable List<EventLoop> eventLoops,
		@Nullable Class<? extends Channel> channelType
	) {
		Builder builder = new Builder();

		if (eventLoops == null || channelType == null) {
			builder.closeEventLoops = true;

			DefaultThreadFactory tf =
				new DefaultThreadFactory("aerospike-proxy", true /*daemon */);

			// TODO: select number of event loop threads?
			EventLoopGroup eventLoopGroup;

			if (Epoll.isAvailable()) {
				eventLoopGroup = new EpollEventLoopGroup(0, tf);
				builder.channelType = EpollSocketChannel.class;
			}
			else {
				eventLoopGroup = new NioEventLoopGroup(0, tf);
				builder.channelType = NioSocketChannel.class;
			}

			builder.eventLoops = StreamSupport.stream(eventLoopGroup.spliterator(), false)
					.map(eventExecutor -> (EventLoop)eventExecutor)
					.collect(Collectors.toList());
		}
		else {
			builder.channelType = channelType;
			builder.eventLoops = eventLoops;
			builder.closeEventLoops = false;
		}

		// TODO: justify defaults.
		builder.maxChannels = 8;

		// Multiple requests should be sent on a stream at once to enhance
		// performance. So `maxConcurrentRequestsPerStream` should be the
		// ideal batch size of the requests.
		// TODO: maybe these parameters depend on the payload size?
		builder.maxConcurrentStreamsPerChannel = 8;
		builder.maxConcurrentRequestsPerStream = builder.totalRequestsPerStream = 128;

		builder.connectTimeoutMillis = 5000;
		builder.terminationWaitMillis = 30000;

		builder.tlsPolicy = null;
		return builder;
	}

	public EventLoop nextEventLoop() {
		int i = eventLoopIndex.getAndIncrement() % eventLoops.size();
		return eventLoops.get(i);
	}

	public static class Builder {
		private List<EventLoop> eventLoops;
		private Class<? extends Channel> channelType;
		private boolean closeEventLoops;
		private int maxChannels;
		private int maxConcurrentStreamsPerChannel;
		private int maxConcurrentRequestsPerStream;
		private int totalRequestsPerStream;
		private int connectTimeoutMillis;
		@Nullable
		private TlsPolicy tlsPolicy;
		@Nullable
		private GrpcChannelSelector grpcChannelSelector;
		@Nullable
		private GrpcStreamSelector grpcStreamSelector;
		@Nullable
		private CallOptions callOptions;
		private long terminationWaitMillis;
		private int closeTimeout;

		private Builder() {
		}

		public GrpcClientPolicy build() {
			if (grpcChannelSelector == null) {
				// TODO: how should low and high water mark be selected?
				int hwm =
						maxConcurrentStreamsPerChannel * maxConcurrentRequestsPerStream;
				int lwm = Math.max(16, (int)(0.8 * hwm));
				grpcChannelSelector =
						new DefaultGrpcChannelSelector(lwm, hwm);
			}

			if (grpcStreamSelector == null) {
				grpcStreamSelector =
						new DefaultGrpcStreamSelector(maxConcurrentStreamsPerChannel, maxConcurrentRequestsPerStream);
			}

			if (callOptions == null) {
				callOptions = CallOptions.DEFAULT;
			}

			return new GrpcClientPolicy(maxChannels, maxConcurrentStreamsPerChannel,
				maxConcurrentRequestsPerStream, totalRequestsPerStream,
				connectTimeoutMillis, terminationWaitMillis, closeTimeout,
				grpcChannelSelector, grpcStreamSelector, callOptions, eventLoops,
				channelType, closeEventLoops, tlsPolicy);
		}

		public Builder maxChannels(int maxChannels) {
			if (maxChannels < 1) {
				throw new IllegalArgumentException(String.format(
						"maxChannels=%d < 1", maxChannels
				));
			}
			this.maxChannels = maxChannels;
			return this;
		}

		public Builder maxConcurrentStreamsPerChannel(int maxConcurrentStreamsPerChannel) {
			if (maxConcurrentStreamsPerChannel < 1) {
				throw new IllegalArgumentException(String.format(
						"maxConcurrentStreamsPerChannel=%d < 1", maxConcurrentStreamsPerChannel
				));
			}
			this.maxConcurrentStreamsPerChannel = maxConcurrentStreamsPerChannel;
			return this;
		}

		public Builder maxConcurrentRequestsPerStream(int maxConcurrentRequestsPerStream) {
			if (maxConcurrentRequestsPerStream < 1) {
				throw new IllegalArgumentException(String.format(
						"maxConcurrentRequestsPerStream=%d < 1", maxConcurrentRequestsPerStream
				));
			}
			this.maxConcurrentRequestsPerStream = maxConcurrentRequestsPerStream;
			return this;
		}

		public Builder totalRequestsPerStream(int totalRequestsPerStream) {
			if (totalRequestsPerStream < 0) {
				throw new IllegalArgumentException(String.format(
						"totalRequestsPerStream=%d < 0", totalRequestsPerStream
				));
			}
			this.totalRequestsPerStream = totalRequestsPerStream;
			return this;
		}

		public Builder connectTimeoutMillis(int connectTimeoutMillis) {
			if (connectTimeoutMillis < 0) {
				throw new IllegalArgumentException(String.format(
					"connectTimeoutMillis=%d < 0", connectTimeoutMillis
				));
			}
			this.connectTimeoutMillis = connectTimeoutMillis;
			return this;
		}

		public Builder closeTimeout(int closeTimeout) {
			this.closeTimeout = closeTimeout;
			return this;
		}

		public Builder tlsPolicy(@Nullable TlsPolicy tlsPolicy) {
			this.tlsPolicy = tlsPolicy;
			return this;
		}

		public Builder grpcChannelSelector(GrpcChannelSelector grpcChannelSelector) {
			this.grpcChannelSelector = grpcChannelSelector;
			return this;
		}

		public Builder grpcStreamSelector(GrpcStreamSelector grpcStreamSelector) {
			this.grpcStreamSelector = grpcStreamSelector;
			return this;
		}

		public Builder callOptions(@Nullable CallOptions callOptions) {
			this.callOptions = callOptions;
			return this;
		}

		public Builder terminationWaitMillis(long terminationWaitMillis) {
			if (terminationWaitMillis < 0) {
				throw new IllegalArgumentException(String.format(
						"terminationWaitMillis=%d < 0", terminationWaitMillis
				));
			}
			this.terminationWaitMillis = terminationWaitMillis;
			return this;
		}
	}
}
