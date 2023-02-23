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
import com.aerospike.client.ResultCode;
import com.aerospike.client.proxy.auth.AuthTokenManager;
import com.aerospike.proxy.client.Kvs;
import io.grpc.ManagedChannel;
import io.netty.channel.EventLoop;

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

	public GrpcCallExecutor(GrpcClientPolicy grpcClientPolicy,
							@Nullable AuthTokenManager authTokenManager,
							Host... hosts) {
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

		try {
			this.channelExecutors =
					IntStream.range(0, grpcClientPolicy.maxChannels).mapToObj(value ->
							GrpcChannelExecutor.newInstance(grpcClientPolicy,
									authTokenManager, hosts)
					).collect(Collectors.toList());
			this.controlChannelExecutors =
					IntStream.range(0, 1).mapToObj(value ->
							GrpcChannelExecutor.newInstance(grpcClientPolicy,
									authTokenManager, hosts)
					).collect(Collectors.toList());
		}
		catch (Exception e) {
			throw new AerospikeException(ResultCode.SERVER_ERROR, e);
		}
	}

	public void execute(GrpcStreamingUnaryCall call) {
		if (totalQueueSize.sum() > maxQueueSize) {
			throw new AerospikeException(ResultCode.NO_MORE_CONNECTIONS,
					"Maximum queue " + maxQueueSize +
							" size exceeded");
		}
		GrpcChannelExecutor executor =
				grpcClientPolicy.grpcChannelSelector.select(channelExecutors,
						call.getStreamingMethodDescriptor());

		// TODO: In case of timeouts, lots of calls will end up filling the
		//  wait queues and timeout once removed for execution from the wait
		//  queue. Have a upper limit on the number of concurrent transactions
		//  per channel and reject this call if all the channels are full.
		totalQueueSize.increment();
		try {
			executor.execute(new WrappedGrpcStreamingUnaryCall(call));
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

	@Override
	public void close() {
		if (isClosed.getAndSet(true)) {
			return;
		}

		isClosed.set(true);
		closeExecutors(channelExecutors);
		closeExecutors(controlChannelExecutors);
	}

	private void closeExecutors(List<GrpcChannelExecutor> executors) {
		// TODO FIX HANG!
		for (GrpcChannelExecutor executor : executors) {
			executor.shutdown();
		}

		if (grpcClientPolicy.closeEventLoops) {
			grpcClientPolicy.eventLoops.stream()
					.map(eventLoop ->
							eventLoop.shutdownGracefully(0,
									grpcClientPolicy.terminationWaitMillis, TimeUnit.MILLISECONDS)

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

		// FIXME: each executor waits for terminationWaitMillis which will be
		//  add up to greater than the terminationWaitMillis.
    	/*
        for (GrpcChannelExecutor executor : executors) {
            executor.shutdown();
        }

        boolean interrupted = false;
        for (GrpcChannelExecutor executor : executors) {
            while (!executor.awaitTermination(grpcClientPolicy.terminationWaitMillis)) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        if (grpcClientPolicy.closeEventLoops) {
            grpcClientPolicy.eventLoops.stream()
                    .map(eventLoop ->
                            eventLoop.shutdownGracefully(0,
                                    grpcClientPolicy.terminationWaitMillis, TimeUnit.MILLISECONDS)

                    ).forEach(future -> {
                                try {
                                    future.await(grpcClientPolicy.terminationWaitMillis);
                                } catch (Exception e) {
                                    // TODO log error?
                                }
                            }
                    );
        }
        */
	}

	private class WrappedGrpcStreamingUnaryCall extends GrpcStreamingUnaryCall {
		WrappedGrpcStreamingUnaryCall(GrpcStreamingUnaryCall delegate) {
			super(delegate);
		}

		@Override
		public void onSuccess(Kvs.AerospikeResponsePayload payload) {
			totalQueueSize.decrement();
			super.onSuccess(payload);
		}

		@Override
		public void onError(Throwable t) {
			totalQueueSize.decrement();
			super.onError(t);
		}
	}
}
