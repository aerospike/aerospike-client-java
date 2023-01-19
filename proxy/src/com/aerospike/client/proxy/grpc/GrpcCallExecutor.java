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

import static java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoop;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.proxy.AerospikeClientProxy;
import com.aerospike.client.proxy.auth.AuthTokenManager;
import com.aerospike.proxy.client.KVSGrpc;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.ManagedChannel;
import io.grpc.NameResolver;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;

/**
 * Executes gRPC calls.
 */
public class GrpcCallExecutor implements Closeable {
    /**
     * Hardcoded list of streaming call names.
     */
    static final List<String> STREAMING_CALLS = Collections.unmodifiableList(
            Lists.newArrayList(KVSGrpc.getGetStreamingMethod().getFullMethodName(),
                    KVSGrpc.getPutStreamingMethod().getFullMethodName()));

    /**
     * Interval at which to poke channels to start calls.
     */
    private static final long POKE_INTERVAL = 1;
    private static final String AEROSPIKE_CLIENT_USER_AGENT =
            "AerospikeClientJava/" + AerospikeClientProxy.Version;
    /**
     * The call queue.
     */
    private final GrpcCallQueue callQueue = new GrpcCallQueue();
    /**
     * Channel call executors.
     */
    private final GrpcChannelExecutor[] channelExecutors;
    /**
     * Scheduler for poking.
     */
    private final ScheduledExecutorService pokeExecutor;
    /*
     * TODO: Temp random choosing.
     */
    private final Random rand = new Random();
    /**
     * Socket timeout in millis.
     */
    private final int connectTimeout;
    /*
     * Executor service for IO threads.
     */
    private final ExecutorService executorService;

    private final EventLoops eventLoops;

    public GrpcCallExecutor(ClientPolicy policy, AuthTokenManager tokenManager, Host... hosts) {
    	EventLoops loops = policy.eventLoops;

    	if (loops == null) {
    		int cores = Runtime.getRuntime().availableProcessors();
    		EventLoopGroup group;
    		EventLoopType type;

	        if (Epoll.isAvailable()) {
	        	// TODO: What is the advantage of providing custom ExecutorService?
	            //group = new EpollEventLoopGroup(cores, executorService);
	            group = new EpollEventLoopGroup(cores);
				type = EventLoopType.NETTY_EPOLL;
	        }
    		else {
	            group = new NioEventLoopGroup(cores);
				type = EventLoopType.NETTY_NIO;
	        }

	        EventPolicy eventPolicy = new EventPolicy();
			this.eventLoops = loops = new NettyEventLoops(eventPolicy, group, type);
    	}
    	else {
    		this.eventLoops = null;

    		if (! (loops instanceof NettyEventLoops)) {
    			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Netty eventLoops are required.");
    		}
    	}

        if (hosts == null || hosts.length < 1) {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR,
                    "need at least one seed host");
        }

        NettyEventLoops nettyLoops = (NettyEventLoops)loops;
        Class<? extends SocketChannel> channelClass = nettyLoops.getSocketChannelClass();
        NettyEventLoop[] eventLoopArray = nettyLoops.getArray();

        this.channelExecutors = new GrpcChannelExecutor[eventLoopArray.length];
        this.connectTimeout = policy.timeout;

        int maxConcurrentStreams = 100;

        try {
            pokeExecutor =
                    Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("grpc-executor").build());
            executorService = createExecutorService();
            for (int i = 0; i < channelExecutors.length; i++) {
                channelExecutors[i] = new GrpcChannelExecutor(createGrpcChannel(eventLoopArray[i], channelClass, hosts, policy.tlsPolicy),
                        maxConcurrentStreams, callQueue, tokenManager);
            }
        }
        catch (Exception e) {
            close();
            throw new AerospikeException(ResultCode.SERVER_ERROR, e);
        }
        scheduledPoke();
    }

    private void scheduledPoke() {
        if (!pokeExecutor.isShutdown()) {
            pokeExecutor.schedule(() -> {
                poke();
                // Schedule the next poke.
                scheduledPoke();
            }, POKE_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Poke all channel executors to start work if they are done.
     */
    private void poke() {
        for (GrpcChannelExecutor channelExecutor : channelExecutors) {
            channelExecutor.poke();
        }
    }

    /**
     * Enqueue a new call for execution.
     *
     * @param call the unary grpc call to enqueue.
     * @throws AerospikeException if the request cannot be enqueued.
     */
    public void enqueue(GrpcStreamingUnaryCall call) throws AerospikeException {
        callQueue.enqueue(call);
        poke();
    }

    private ExecutorService createExecutorService() {
        // TODO: get thread pool size from client policy?
        return
                new ForkJoinPool(2 * Runtime.getRuntime().availableProcessors(),
                        new ForkJoinPool.ForkJoinWorkerThreadFactory() {
                            final AtomicInteger num = new AtomicInteger();

                            @Override
                            public ForkJoinWorkerThread newThread(
                                    ForkJoinPool forkJoinPool) {
                                ForkJoinWorkerThread thread = defaultForkJoinWorkerThreadFactory.newThread(
                                        forkJoinPool);
                                thread.setDaemon(true);
                                thread.setName("aerospike-client-grpc" + num.getAndIncrement());
                                return thread;
                            }
                        }, (t, e) -> Log.debug(String.format("Uncaught exception: %s", e)), true);
    }


    /**
     * Create a gRPC channel
     *
     * @param hosts     the list of hosts each channel load balances across
     * @param tlsPolicy tls policy is TLS required, else null
     * @return a nee gRPC channel
     */
    @SuppressWarnings("deprecation")
    private ManagedChannel createGrpcChannel(NettyEventLoop eventLoop, Class<? extends SocketChannel> channelClass, Host[] hosts, TlsPolicy tlsPolicy) {
        NettyChannelBuilder builder;

        if (hosts.length == 1) {
            builder = NettyChannelBuilder.forAddress(hosts[0].name, hosts[0].port);
        } else {
            // Setup round-robin load balancing.
            NameResolver.Factory nameResolverFactory = new MultiAddressNameResolverFactory(
                    Arrays.stream(hosts)
                            .map((host) -> new InetSocketAddress(host.name, host.port))
                            .collect(
                                    Collectors.<SocketAddress>toList()));
            builder = NettyChannelBuilder.forTarget(String.format("%s:%d",
                    hosts[0].name, hosts[0].port));
            builder.nameResolverFactory(nameResolverFactory);
            builder.defaultLoadBalancingPolicy("round_robin");
        }

        builder
                .eventLoopGroup(eventLoop.get())
                .perRpcBufferLimit(134217728L)
                .channelType(channelClass)
                .negotiationType(NegotiationType.PLAINTEXT)
                .flowControlWindow(2 * 1024 * 1024)
                .initialFlowControlWindow(1024 * 1024)
                .keepAliveWithoutCalls(true)
                .keepAliveTime(1, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .executor(executorService)
                .offloadExecutor(executorService)
                .disableRetry();

        if (tlsPolicy != null) {
            builder.sslContext(getSslContext(tlsPolicy));
        } else {
            builder.usePlaintext();
        }

        //setting buffer size can improve I/O
        builder.withOption(ChannelOption.SO_SNDBUF, 1048576);
        builder.withOption(ChannelOption.SO_RCVBUF, 1048576);
        builder.withOption(ChannelOption.TCP_NODELAY, true);
        builder.withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
        builder.userAgent(AEROSPIKE_CLIENT_USER_AGENT);

        // better to have a receive buffer predictor
        //builder.withOption(ChannelOption.valueOf("receiveBufferSizePredictorFactory"), new AdaptiveReceiveBufferSizePredictorFactory(MIN_PACKET_SIZE, INITIAL_PACKET_SIZE, MAX_PACKET_SIZE))

        //if the server is sending 1000 messages per sec, optimum write buffer watermarks will
        //prevent unnecessary throttling, Check NioSocketChannelConfig doc
        builder.withOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                new WriteBufferWaterMark(32 * 1024, 64 * 1024));

        return builder.build();
    }

    private SslContext getSslContext(TlsPolicy tlsPolicy) {
        return new JdkSslContext(tlsPolicy.context, true,
                tlsPolicy.ciphers != null ? Arrays.stream(tlsPolicy.ciphers).collect(
                        Collectors.toList()) : null,
                IdentityCipherSuiteFilter.INSTANCE,
                ApplicationProtocolConfig.DISABLED, ClientAuth.NONE,
                tlsPolicy.protocols, true);
    }

    public void close() {
        pokeExecutor.shutdown();

        for (GrpcChannelExecutor channelExecutor : channelExecutors) {
            try {
                channelExecutor.getChannel().shutdownNow();
            }
            catch (Exception ignored) {
            }
        }

        try {
            executorService.shutdownNow();
        }
        catch (Exception ignored) {
        }

        // Close eventLoops if they were created internally.
        if (eventLoops != null) {
        	eventLoops.close();
        }
    }

    /**
     * @return return a random channel from created channels.
     */
    public ManagedChannel getChannel() {
        int randIndex = rand.nextInt(channelExecutors.length);
        return channelExecutors[randIndex].getChannel();
    }
}
