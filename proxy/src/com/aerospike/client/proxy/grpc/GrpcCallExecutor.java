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

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.proxy.auth.AuthTokenManager;
import com.aerospike.proxy.client.Kvs;

import io.grpc.ManagedChannel;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

public class GrpcCallExecutor implements Closeable {
	private static final int QUEUE_SIZE_UPPER_BOUND = 100 * 1024;
	private final List<GrpcChannelExecutor> channelExecutors;
	private final List<GrpcChannelExecutor> controlChannelExecutors;
	private final GrpcClientPolicy grpcClientPolicy;
	private final Random random = new Random();
	private final AtomicBoolean isClosed = new AtomicBoolean(false);

	/**
	 * Maximum allowed queue size.
	 */
	private final int maxQueueSize;

	private final LongAdder totalQueueSize = new LongAdder();
	private final GrpcChannelExecutor.ChannelTypeAndEventLoop controlChannelTypeAndEventLoop;

	public GrpcCallExecutor(
		GrpcClientPolicy grpcClientPolicy,
		@Nullable AuthTokenManager authTokenManager,
		Host... hosts
	) {
		if (hosts == null || hosts.length < 1) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"need at least one seed host");
		}

		this.grpcClientPolicy = grpcClientPolicy;
		maxQueueSize =
				Math.min(QUEUE_SIZE_UPPER_BOUND,
						5 * grpcClientPolicy.maxChannels
						  * grpcClientPolicy.maxConcurrentStreamsPerChannel
						  * grpcClientPolicy.maxConcurrentRequestsPerStream);

		this.controlChannelTypeAndEventLoop = getControlEventLoop();

		try {
			this.channelExecutors =
					IntStream.range(0, grpcClientPolicy.maxChannels).mapToObj(value ->
							new GrpcChannelExecutor(grpcClientPolicy,
									new GrpcChannelExecutor.ChannelTypeAndEventLoop(grpcClientPolicy.channelType,
											grpcClientPolicy.nextEventLoop()),
									authTokenManager, hosts)
					).collect(Collectors.toList());
			this.controlChannelExecutors =
					IntStream.range(0, 1).mapToObj(value ->
							new GrpcChannelExecutor(grpcClientPolicy,
									controlChannelTypeAndEventLoop, authTokenManager,
									hosts)
					).collect(Collectors.toList());
		}
		catch (Exception e) {
			throw new AerospikeException(ResultCode.SERVER_ERROR, e);
		}
	}

    public void execute(GrpcStreamingCall call) {
		if (totalQueueSize.sum() > maxQueueSize) {
			call.onError(new AerospikeException(ResultCode.NO_MORE_CONNECTIONS,
				"Maximum queue " + maxQueueSize + " size exceeded"));
			return;
		}

		GrpcChannelExecutor executor =
			grpcClientPolicy.grpcChannelSelector.select(channelExecutors, call);

		// TODO: In case of timeouts, lots of calls will end up filling the
		//  wait queues and timeout once removed for execution from the wait
		//  queue. Have a upper limit on the number of concurrent transactions
		//  per channel and reject this call if all the channels are full.
		totalQueueSize.increment();

		try {
			executor.execute(new WrappedGrpcStreamingCall(call));
		}
		catch (Exception e) {
			// Call scheduling failed.
			totalQueueSize.decrement();
		}
	}

	public EventLoop getEventLoop() {
		return channelExecutors.get(random.nextInt(channelExecutors.size()))
				.getEventLoop();
	}

	public ManagedChannel getControlChannel() {
		if (controlChannelExecutors.isEmpty()) {
			return null;
		}
		return controlChannelExecutors.get(random.nextInt(controlChannelExecutors.size()))
				.getChannel();
	}

    public ManagedChannel getChannel() {
        if(channelExecutors.isEmpty()) {
            return null;
        }
        return channelExecutors.get(random.nextInt(channelExecutors.size()))
                .getChannel();
    }

    @Override
    public void close() {
		if (isClosed.getAndSet(true)) {
			return;
		}

		closeExecutors(channelExecutors);
		closeExecutors(controlChannelExecutors);

		// Event loops should be closed after shutdown of channels.
		closeEventLoops();
	}

	private GrpcChannelExecutor.ChannelTypeAndEventLoop getControlEventLoop() {
		EventLoopGroup eventLoopGroup;
		Class<? extends Channel> channelType;
		DefaultThreadFactory tf = new DefaultThreadFactory("aerospike-proxy-control", true /*daemon*/);

		if (Epoll.isAvailable()) {
			eventLoopGroup = new EpollEventLoopGroup(1, tf);
			channelType = EpollSocketChannel.class;
		}
		else {
			eventLoopGroup = new NioEventLoopGroup(1, tf);
			channelType = NioSocketChannel.class;
		}

		return new GrpcChannelExecutor.ChannelTypeAndEventLoop(channelType, (EventLoop)eventLoopGroup.iterator().next());
	}

	private void closeExecutors(List<GrpcChannelExecutor> executors) {
		for (GrpcChannelExecutor executor : executors) {
			executor.shutdown();
		}

		// Wait for all executors to terminate.
		while (true) {
			boolean allTerminated = executors.stream()
				.allMatch(GrpcChannelExecutor::isTerminated);

			if (allTerminated) {
				return;
			}

			Log.debug("Waiting for executors to shutdown with closeTimeout=" + grpcClientPolicy.closeTimeout);
			try {
				//noinspection BusyWait
				Thread.sleep(1000);
			}
			catch (Throwable t) {/* Ignore*/}
		}
	}


	private void closeEventLoops() {
		if (grpcClientPolicy.closeEventLoops) {
			closeEventLoops(grpcClientPolicy.eventLoops);
		}

		// Close the control event loop.
		closeEventLoops(Collections.singletonList(controlChannelTypeAndEventLoop.getEventLoop()));
	}

	private void closeEventLoops(List<EventLoop> eventLoops) {
		eventLoops.stream()
			.map(eventLoop ->
				eventLoop.shutdownGracefully(0, grpcClientPolicy.terminationWaitMillis, TimeUnit.MILLISECONDS)
			).forEach(future -> {
					try {
						future.await(grpcClientPolicy.terminationWaitMillis);
					}
					catch (Exception e) {
						// TODO log error?
					}
				}
			);
	}

	private class WrappedGrpcStreamingCall extends GrpcStreamingCall {
		WrappedGrpcStreamingCall(GrpcStreamingCall delegate) {
			super(delegate);
		}

		@Override
		public void onNext(Kvs.AerospikeResponsePayload payload) {
			if (!payload.getHasNext()) {
				totalQueueSize.decrement();
			}
			super.onNext(payload);
		}

		@Override
		public void onError(Throwable t) {
			totalQueueSize.decrement();
			super.onError(t);
		}
	}
}
