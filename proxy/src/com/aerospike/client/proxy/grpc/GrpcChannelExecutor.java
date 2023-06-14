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

import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.KeyManagerFactory;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.proxy.AerospikeClientProxy;
import com.aerospike.client.proxy.auth.AuthTokenManager;
import com.aerospike.client.util.Util;
import com.aerospike.proxy.client.Kvs;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.NameResolver;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;

/**
 * All gRPC requests on a HTTP/2 channel are handled by this class throughout
 * the channel lifetime.
 * <p>
 * TODO: handle close of channel.
 */
public class GrpcChannelExecutor implements Runnable {
	/**
	 * System property to configure gRPC override authority used as hostname
	 * in TLS verification of the proxy server.
	 */
	public static final String OVERRIDE_AUTHORITY = "com.aerospike.client" +
		".overrideAuthority";

	private static final String AEROSPIKE_CLIENT_USER_AGENT =
		"AerospikeClientJava/" + AerospikeClientProxy.Version;

	/**
	 * The delay between iterations of this executor.
	 * <p>
	 * TODO: how to select interval of execution?
	 */
	private static final long ITERATION_DELAY_MICROS = 250;

	/**
	 * Unique executor ids.
	 */
	private static final AtomicLong executorIdIndex = new AtomicLong();
	private static final AtomicInteger streamIdIndex = new AtomicInteger();

	/**
	 * The HTTP/2 channel of this executor.
	 */
	private final ManagedChannel channel;
	/**
	 * The Aerospike gRPC client policy.
	 */
	private final GrpcClientPolicy grpcClientPolicy;
	/**
	 * The auth token manager.
	 */
	private final AuthTokenManager authTokenManager;
	/**
	 * The event loop bound to the <code>channel</code>. All queued requests
	 * will be executed on this event loop. Some requests will be queued on
	 * this channel in the gRPC callback and some from the pending queue.
	 */
	private final EventLoop eventLoop;
	/**
	 * Queued unary calls awaiting execution.
	 */
	private final MpscUnboundedArrayQueue<GrpcStreamingCall> pendingCalls =
		new MpscUnboundedArrayQueue<>(32);
	/**
	 * Queue of closed streams.
	 */
	private final List<GrpcStream> closedStreams = new ArrayList<>(32);
	/**
	 * Map of stream id to streams.
	 */
	private final Map<Integer, GrpcStream> streams = new HashMap<>();
	/**
	 * Shutdown initiation time.
	 */
	private long shutdownStartTimeNanos;
	/**
	 * Current state of the channel.
	 */
	private final AtomicReference<ChannelState> channelState;
	/**
	 * Unique id of the executor.
	 */
	private final long id;
	// Statistics.
	private final AtomicLong ongoingRequests = new AtomicLong();
	private final int drainLimit;

	/**
	 * The future to cancel the scheduled iteration of this executor.
	 */
	private ScheduledFuture<?> iterateFuture;

	/**
	 * Time when the channel executor saw an invalid token. If this field is
	 * zero the token is valid.
	 * <p>
	 * Is not volatile because it is access from a single thread.
	 */
	private long tokenInvalidStartTime = 0;

	public GrpcChannelExecutor(
		GrpcClientPolicy grpcClientPolicy,
		ChannelTypeAndEventLoop channelTypeAndEventLoop,
		@Nullable AuthTokenManager authTokenManager,
		Host... hosts
	) {
		if (grpcClientPolicy == null) {
			throw new NullPointerException("grpcClientPolicy");
		}
		if (hosts == null || hosts.length == 0) {
			throw new IllegalArgumentException("hosts should be non-empty");
		}

		this.grpcClientPolicy = grpcClientPolicy;
		this.drainLimit =
			this.grpcClientPolicy.maxConcurrentStreamsPerChannel * grpcClientPolicy.maxConcurrentRequestsPerStream;
		this.authTokenManager = authTokenManager;
		this.id = executorIdIndex.getAndIncrement();

		ChannelAndEventLoop channelAndEventLoop =
			createGrpcChannel(channelTypeAndEventLoop.getEventLoop()
				, channelTypeAndEventLoop.getChannelType(), hosts);
		this.channel = channelAndEventLoop.managedChannel;
		this.eventLoop = channelAndEventLoop.eventLoop;

		this.channelState = new AtomicReference<>(ChannelState.READY);

		this.iterateFuture =
			channelAndEventLoop.eventLoop.scheduleAtFixedRate(this, 0,
				ITERATION_DELAY_MICROS, TimeUnit.MICROSECONDS);
	}

	private static SslContext getSslContext(TlsPolicy tlsPolicy) {
		try {
			SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
			Field field = sslContextBuilder.getClass().getDeclaredField("apn");
			field.setAccessible(true);
			ApplicationProtocolConfig applicationProtocolConfig = (ApplicationProtocolConfig)field.get(sslContextBuilder);

			if (tlsPolicy.context != null) {
				CipherSuiteFilter csf = (tlsPolicy.ciphers != null) ? (iterable, list, set) -> {
					if (tlsPolicy.ciphers != null) {
						return tlsPolicy.ciphers;
					}
					return tlsPolicy.context.getSupportedSSLParameters().getCipherSuites();
				} : IdentityCipherSuiteFilter.INSTANCE;

				// Enforce ALPN in case NPN_AND_ALPN is the supported protocol.
				// JdkSslContext fails with an exception when the protocol is
				// NPN_AND_ALPN.
				ApplicationProtocolConfig apn = applicationProtocolConfig;
				if (applicationProtocolConfig.protocol() == ApplicationProtocolConfig.Protocol.NPN_AND_ALPN) {
					// Constructor copied verbatim from package-private field
					// io.grpc.netty.GrpcSslContexts.ALPN
					apn = new ApplicationProtocolConfig(
						ApplicationProtocolConfig.Protocol.ALPN,
						ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
						ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
						Collections.singletonList("h2"));
				}

				return new JdkSslContext(tlsPolicy.context, true, null, csf, apn, ClientAuth.NONE, null, false);
			}

			SslContextBuilder builder = SslContextBuilder.forClient();
			builder.applicationProtocolConfig(applicationProtocolConfig);
			if (tlsPolicy.protocols != null) {
				builder.protocols(tlsPolicy.protocols);
			}

			if (tlsPolicy.ciphers != null) {
				builder.ciphers(Arrays.asList(tlsPolicy.ciphers));
			}

			String keyStoreLocation = System.getProperty("javax.net.ssl.keyStore");

			// Keystore is only required for mutual authentication.
			if (keyStoreLocation != null) {
				String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
				char[] pass = (keyStorePassword != null) ? keyStorePassword.toCharArray() : null;

				KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());

				try (FileInputStream is = new FileInputStream(keyStoreLocation)) {
					ks.load(is, pass);
				}

				KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
				kmf.init(ks, pass);

				builder.keyManager(kmf);
			}
			return builder.build();
		}
		catch (Exception e) {
			throw new AerospikeException("Failed to init netty TLS: " + Util.getErrorMessage(e));
		}
	}

	/**
	 * Create a gRPC channel.
	 */
	@SuppressWarnings("deprecation")
	private ChannelAndEventLoop createGrpcChannel(EventLoop eventLoop, Class<? extends Channel> channelType, Host[] hosts) {
		NettyChannelBuilder builder;

		if (hosts.length == 1) {
			builder = NettyChannelBuilder.forAddress(hosts[0].name, hosts[0].port);
		}
		else {
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

		SingleEventLoopGroup eventLoopGroup = new SingleEventLoopGroup(eventLoop);
		builder
			.eventLoopGroup(eventLoopGroup)
			.perRpcBufferLimit(128 * 1024 * 1024)
			.channelType(channelType)
			.negotiationType(NegotiationType.PLAINTEXT)

			// Have a very large limit because this response is coming from
			// the proxy server.
			.maxInboundMessageSize(128 * 1024 * 1024)

			// Execute callbacks in the assigned event loop.
			// GrpcChannelExecutor.iterate and all of GrpcStream works on
			// this assumption.
			.directExecutor()

			// Retry logic is part of the client code.
			.disableRetry()

			// Server and client flow control policy should be in sync.
			.flowControlWindow(2 * 1024 * 1024)

			// TODO: is this beneficial? See https://github.com/grpc/grpc-java/issues/8260
			//  for discussion.
			// Enabling this feature create too many pings and the server
			// sends GO_AWAY response.
			// .initialFlowControlWindow(1024 * 1024)

			// TODO: Should these be part of GrpcClientPolicy?
			.keepAliveWithoutCalls(true)
			.keepAliveTime(25, TimeUnit.SECONDS)
			.keepAliveTimeout(1, TimeUnit.MINUTES);

		if (grpcClientPolicy.tlsPolicy != null) {
			builder.sslContext(getSslContext(grpcClientPolicy.tlsPolicy));
			builder.negotiationType(NegotiationType.TLS);
		}
		else {
			builder.usePlaintext();
		}

		// For testing. Set this to force a hostname irrespective of the
		// target IP for TLS verification. A simpler way than adding a DNS
		// entry in the hosts file.
		String authorityProperty = System.getProperty(OVERRIDE_AUTHORITY);

		if (authorityProperty != null && !authorityProperty.trim().isEmpty()) {
			builder.overrideAuthority(authorityProperty);
		}

		//setting buffer size can improve I/O
		builder.withOption(ChannelOption.SO_SNDBUF, 1048576);
		builder.withOption(ChannelOption.SO_RCVBUF, 1048576);
		builder.withOption(ChannelOption.TCP_NODELAY, true);
		builder.withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS,
			grpcClientPolicy.connectTimeoutMillis);
		builder.userAgent(AEROSPIKE_CLIENT_USER_AGENT);

		// TODO: better to have a receive buffer predictor
		//builder.withOption(ChannelOption.valueOf("receiveBufferSizePredictorFactory"), new AdaptiveReceiveBufferSizePredictorFactory(MIN_PACKET_SIZE, INITIAL_PACKET_SIZE, MAX_PACKET_SIZE))

		//if the server is sending 1000 messages per sec, optimum write buffer watermarks will
		//prevent unnecessary throttling, Check NioSocketChannelConfig doc
		builder.withOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
			new WriteBufferWaterMark(32 * 1024, 64 * 1024));

		ManagedChannel channel = builder.build();
		// TODO: ensure it is a single threaded event loop.
		return new ChannelAndEventLoop(channel, eventLoop);
	}

	public void execute(GrpcStreamingCall call) {
		if (channelState.get() != ChannelState.READY) {
			call.failIfNotComplete(ResultCode.CLIENT_ERROR);
			return;
		}
		// TODO: add always succeeds?
		ongoingRequests.getAndIncrement();
		pendingCalls.add(call);
	}

	@Override
	public void run() {
		try {
			iterate();
		}
		catch (Exception e) {
			// TODO: signal failure, close channel?
			if (Log.debugEnabled()) {
				Log.debug("Uncaught exception in " + this + ":" + e);
			}
		}
	}

	/**
	 * Process a single iteration.
	 */
	private void iterate() {
		switch (channelState.get()) {
			case READY:
				executeCalls();
				break;

			case SHUTTING_DOWN:
				boolean allCallsCompleted = pendingCalls.isEmpty() &&
					streams.values().stream()
						.allMatch(grpcStream -> grpcStream.getOngoingRequests() == 0);

				int closeTimeout = grpcClientPolicy.closeTimeout;
				if (closeTimeout < 0) {
					// Shutdown immediately.
					shutdownNow();
				}
				else if (closeTimeout == 0) {
					// Wait for all pending calls to complete.
					if (allCallsCompleted) {
						shutdownNow();
					}
					else {
						Log.debug(this + " shutdown: awaiting completion of " +
							"all calls for closeTimeout=0.");
						executeCalls();
					}
				}
				else {
					// Wait for all pending calls to complete or timeout.
					long elapsedTimeMillis =
						TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - shutdownStartTimeNanos);
					if (allCallsCompleted || elapsedTimeMillis >= closeTimeout) {
						shutdownNow();
					}
					else {
						Log.debug(this + " shutdown: awaiting closeTimeout="
							+ closeTimeout + ", elapsed time=" + elapsedTimeMillis);
						executeCalls();
					}
				}
				break;

			case SHUTDOWN:
				Log.warn("Iterate being called after channel shutdown");
				break;

			default:
				Log.error("Unknown channel state: " + channelState.get());
				break;
		}
	}

	private void executeCalls() {
		if (authTokenManager != null) {
			AuthTokenManager.TokenStatus tokenStatus =
				authTokenManager.getTokenStatus();
			if (!tokenStatus.isValid()) {
				expireOrDrainOnInvalidToken(tokenStatus.getError());
				return;
			}
		}

		// Schedule pending calls onto streams.
		pendingCalls.drain(this::scheduleCalls, drainLimit);

		// Execute stream calls.
		streams.values().forEach(GrpcStream::executePendingCalls);

		// Process closed streams.
		closedStreams.forEach(this::processClosedStream);
		closedStreams.clear();
	}

	/**
	 * Expire queued calls and drain queue if required when we have an invalid
	 * auth token.
	 */
	private void expireOrDrainOnInvalidToken(Throwable tokenError) {
		assert authTokenManager != null;

		if (tokenInvalidStartTime == 0) {
			tokenInvalidStartTime = System.currentTimeMillis();
		}

		// Token is invalid. This happens at the start before the first
		// access token fetch or if the token expires and could not be
		// refreshed.
		pendingCalls.forEach(call -> {
			if (!call.hasCompleted() &&
				(call.hasSendDeadlineExpired() || call.hasExpired())) {
				call.onError(tokenError);
			}
		});


		long tokenWaitTimeout = tokenInvalidStartTime + authTokenManager.getRefreshMinTime() * 3L;

		if (tokenWaitTimeout < System.currentTimeMillis()) {
			tokenInvalidStartTime = 0;
			// It's been too long without a valid access token. Drain and
			// report all queued calls as failed.
			pendingCalls.drain(call -> call.failIfNotComplete(tokenError));
		}
	}

	/**
	 * Schedule the call on a stream.
	 */
	private void scheduleCalls(GrpcStreamingCall call) {
		if (call.hasCompleted()) {
			// Most likely expired while in queue.
			return;
		}

		if (call.hasSendDeadlineExpired() || call.hasExpired()) {
			call.onError(new AerospikeException.Timeout(call.getPolicy(),
				call.getIteration()));
			return;
		}

		// The stream will be close by the selector.
		GrpcStreamSelector.SelectedStream selectedStream =
			grpcClientPolicy.grpcStreamSelector.select(new ArrayList<>(streams.values()), call);

		if (selectedStream == null) {
			// Requeue
			pendingCalls.add(call);
			return;
		}

		if (selectedStream.useExistingStream()) {
			selectedStream.getStream().enqueue(call);
			return;
		}

		scheduleCallsOnNewStream(call.getStreamingMethodDescriptor(), call,
			selectedStream.getMaxConcurrentRequestsPerStream(),
			selectedStream.getTotalRequestsPerStream());
	}

	private void processClosedStream(GrpcStream grpcStream) {
		if (streams.remove(grpcStream.getId()) == null) {
			// Should never happen.
			return;
		}

		// Schedule each of the pending calls.
		pendingCalls.addAll(grpcStream.getPendingCalls());
	}

	/**
	 * Schedule calls in pendingCalls on a new stream.
	 */
	private void scheduleCallsOnNewStream(
		MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor,
		GrpcStreamingCall call,
		int maxConcurrentRequestsPerStream, int totalRequestsPerStream
	) {
		if (maxConcurrentRequestsPerStream <= 0) { // Should never happen.
			maxConcurrentRequestsPerStream =
				grpcClientPolicy.maxConcurrentRequestsPerStream;
		}
		if (totalRequestsPerStream <= 0) { // Should never happen.
			totalRequestsPerStream = grpcClientPolicy.totalRequestsPerStream;
		}

		CallOptions options = grpcClientPolicy.callOptions;
		if (authTokenManager != null) {
			try {
				options = authTokenManager.setCallCredentials(grpcClientPolicy.callOptions);
			}
			catch (Exception e) {
				AerospikeException aerospikeException =
					new AerospikeException(ResultCode.NOT_AUTHENTICATED, e);
				call.onError(aerospikeException);
				return;
			}
		}

		LinkedList<GrpcStreamingCall> streamPendingCalls = new LinkedList<>();
		streamPendingCalls.add(call);
		GrpcStream stream = new GrpcStream(this, methodDescriptor,
			streamPendingCalls, options, nextStreamId(), eventLoop,
			maxConcurrentRequestsPerStream, totalRequestsPerStream);

		streams.put(stream.getId(), stream);
	}

	/**
	 * Start the shutdown of this channel. Any new requests will be rejected.
	 * The shutdown respects the clientTimeout setting. Use
	 * {@link #isTerminated()} to see if shutdown is complete.
	 */
	public void shutdown() {
		if (!channelState.compareAndSet(ChannelState.READY, ChannelState.SHUTTING_DOWN)) {
			return;
		}

		shutdownStartTimeNanos = System.nanoTime();

		// If inside event loop thread, cannot wait for calls in this channel
		// to complete without deadlocking, abort and shutdown now.
		if (eventLoop.inEventLoop()) {
			shutdownNow();
		}
	}

	/**
	 * <b>WARN</b> This method should always be called from the [eventLoop]
	 * thread.
	 */
	private void shutdownNow() {
		if (channelState.getAndSet(ChannelState.SHUTDOWN) == ChannelState.SHUTDOWN) {
			return;
		}

		closeAllPendingCalls();
		channel.shutdownNow();
		iterateFuture.cancel(false);
	}

	private void closeAllPendingCalls() {
		while (!pendingCalls.isEmpty()) {
			pendingCalls.drain(call -> {
				try {
					call.failIfNotComplete(ResultCode.CLIENT_ERROR);
				}
				catch (Exception e) {
					Log.error("Error on call close " + call + ": " + e.getMessage());
				}
			});
		}
		streams.values().forEach(stream -> {
			try {
				stream.closePendingCalls();
			}
			catch (Exception e) {
				Log.error("Error closing stream " + stream + ": " + e.getMessage());
			}
		});
		streams.clear();
	}

	boolean isTerminated() {
		return channelState.get() == ChannelState.SHUTDOWN && channel.isTerminated();
	}

	private int nextStreamId() {
		return streamIdIndex.getAndIncrement();
	}

	@Override
	public String toString() {
		return "GrpcChannelExecutor{id=" + id + '}';
	}

	public long getId() {
		return id;
	}

	public long getOngoingRequests() {
		return ongoingRequests.get();
	}

	void onRequestCompleted() {
		ongoingRequests.getAndDecrement();
	}

	public void onStreamClosed(GrpcStream grpcStream) {
		closedStreams.add(grpcStream);
	}

	public ManagedChannel getChannel() {
		return channel;
	}

	public EventLoop getEventLoop() {
		return eventLoop;
	}

	private static class ChannelAndEventLoop {
		final ManagedChannel managedChannel;
		final EventLoop eventLoop;

		private ChannelAndEventLoop(ManagedChannel managedChannel, EventLoop eventLoop) {
			this.managedChannel = managedChannel;
			this.eventLoop = eventLoop;
		}
	}

	private enum ChannelState {
		READY, SHUTTING_DOWN, SHUTDOWN
	}

	public static class ChannelTypeAndEventLoop {
		private final Class<? extends Channel> channelType;
		private final EventLoop eventLoop;

		public ChannelTypeAndEventLoop(Class<? extends Channel> channelType, EventLoop eventLoop) {
			this.channelType = channelType;
			this.eventLoop = eventLoop;
		}

		public Class<? extends Channel> getChannelType() {
			return channelType;
		}

		public EventLoop getEventLoop() {
			return eventLoop;
		}
	}
}
