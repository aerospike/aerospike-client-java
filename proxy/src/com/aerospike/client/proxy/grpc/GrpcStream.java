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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.proxy.client.Kvs;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoop;

/**
 * This class executes a single Aerospike API method like get, put, etc.
 * throughout its lifetime. It executes a maximum of `totalRequestsPerStream`
 * before closing the stream.
 * <p>
 * <em>NOTE</em> All methods of the stream are executed within a single
 * thread. This is implemented by
 * <ul>
 *     <li>having the channel configured to use the direct executor</li>
 *     <li>have the channel and streams associated with the channel be
 *     executed on a single event loop</li>
 * </ul>
 */
public class GrpcStream implements StreamObserver<Kvs.AerospikeResponsePayload> {
	/**
	 * Idle timeout after which the stream is closed, when no call are pending.
	 */
	private static final long IDLE_TIMEOUT = 30_000;

	/**
	 * Unique stream id in the channel.
	 */
	private final int id;

	/**
	 * The event loop within which all of GrpcStream calls are executed.
	 */
	private final EventLoop eventLoop;

	/**
	 * The request observer of the stream.
	 */
	private StreamObserver<Kvs.AerospikeRequestPayload> requestObserver;

	/**
	 * Maximum number of concurrent requests that can be in-flight.
	 */
	private final int maxConcurrentRequests;

	/**
	 * Total number of requests to process in this stream for its lifetime.
	 */
	private final int totalRequestsToExecute;

	/**
	 * The executor for this stream.
	 */
	private final GrpcChannelExecutor channelExecutor;

	/**
	 * The method processed by this stream.
	 */
	private final MethodDescriptor<Kvs.AerospikeRequestPayload,
		Kvs.AerospikeResponsePayload> methodDescriptor;

	/**
	 * Queued calls pending execution.
	 * <p>
	 * <b>WARN</b> Ensure this is always accessed from the {@link #eventLoop}
	 * thread.
	 */
	private final LinkedList<GrpcStreamingCall> pendingCalls;

	/**
	 * Map of request id to the calls executing in this stream.
	 */
	private final Map<Integer, GrpcStreamingCall> executingCalls = new HashMap<>();

	/**
	 * Is the stream closed. This variable is only accessed from the event
	 * loop thread assigned to this stream and its channel.
	 */
	private boolean isClosed = false;

	// Stream statistics. These are only updated from the event loop thread
	// assigned to this stream and its channel.

	/**
	 * Number of requests sent on the gRPC stream. This is only updated from
	 * the event loop thread assigned to this stream and its channel.
	 */
	private volatile int requestsSent;

	/**
	 * Number of requests completed. This is only updated from
	 * the event loop thread assigned to this stream and its channel.
	 */
	private volatile int requestsCompleted;

	/**
	 * Timer started when this stream has no pending calls.
	 * There may still be calls executing (in-flight).
	 */
	private volatile long streamIdleStartTime;

	/**
	 * Indicates if the gRPC stream has been half closed from this side.
	 */
	private boolean streamHalfClosed;

	public GrpcStream(
		GrpcChannelExecutor channelExecutor,
		MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor,
		LinkedList<GrpcStreamingCall> pendingCalls,
		CallOptions callOptions,
		int streamIndex,
		EventLoop eventLoop,
		int maxConcurrentRequests,
		int totalRequestsToExecute
	) {
		this.channelExecutor = channelExecutor;
		this.methodDescriptor = methodDescriptor;
		this.pendingCalls = pendingCalls;
		this.id = streamIndex;
		this.eventLoop = eventLoop;
		this.maxConcurrentRequests = maxConcurrentRequests;
		this.totalRequestsToExecute = totalRequestsToExecute;
		ClientCall<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> call = channelExecutor.getChannel()
			.newCall(methodDescriptor, callOptions);
		StreamObserver<Kvs.AerospikeRequestPayload> requestObserver =
			ClientCalls.asyncBidiStreamingCall(call, this);
		setRequestObserver(requestObserver);
	}

	private void setRequestObserver(StreamObserver<Kvs.AerospikeRequestPayload> requestObserver) {
		this.requestObserver = requestObserver;
	}

	@Override
	public void onNext(Kvs.AerospikeResponsePayload aerospikeResponsePayload) {
		if (!eventLoop.inEventLoop()) {
			// This call is not within the event loop thread. For some reason
			// gRPC invokes some callbacks from a different thread.
			eventLoop.schedule(() -> onNext(aerospikeResponsePayload), 0,
				TimeUnit.NANOSECONDS);
			return;
		}

		// Invoke callback.
		int callId = aerospikeResponsePayload.getId();
		GrpcStreamingCall call;

		if (aerospikeResponsePayload.getHasNext()) {
			call = executingCalls.get(callId);
		}
		else {
			call = executingCalls.remove(callId);

			// Update stats.
			requestsCompleted++;
			channelExecutor.onRequestCompleted();
		}

		// Call might have expired and been cancelled.
		if (call != null && !call.isAborted()) {
			try {
				call.onNext(aerospikeResponsePayload);
			}
			catch (Throwable t) {
				if (aerospikeResponsePayload.getHasNext()) {
					abortCallAtServer(call, callId);
				}
			}
		}

		executePendingCalls();
	}

	private void abortCallAtServer(GrpcStreamingCall call, int callId) {
		call.markAborted();

		// Let the proxy know that there has been a failure so that
		// it can abort long-running jobs.
		int requestId = requestsSent++;
		Kvs.AerospikeRequestPayload.Builder builder = Kvs.AerospikeRequestPayload.newBuilder();
		builder.setId(requestId);
		builder.setAbortRequest(Kvs.AbortRequest.newBuilder().setAbortId(callId));
		requestObserver.onNext(builder.build());
	}

	private void abortExecutingCalls(Throwable throwable) {
		isClosed = true;

		for (GrpcStreamingCall call : executingCalls.values()) {
			try {
				call.onError(throwable);
			}
			catch (Exception e) {
				Log.debug("Exception in invoking onError: " + e);
			}
		}

		markClosed();
	}

	/**
	 * Marks the stream as closed and moves pending calls nack to the channel
	 * executor.
	 */
	private void markClosed() {
		isClosed = true;

		executingCalls.clear();

		channelExecutor.onStreamClosed(this);
	}

	@Override
	public void onError(Throwable throwable) {
		if (!eventLoop.inEventLoop()) {
			// This call is not within the event loop thread. For some reason
			// gRPC invokes error callback from a different thread.
			eventLoop.schedule(() -> onError(throwable), 0,
				TimeUnit.NANOSECONDS);
			return;
		}

		if (executingCalls.isEmpty()) {
			// gRPC stream creation failed.
			// Fail all pending calls, otherwise they will keep cycling
			// through until a stream creation succeeds.
			for (GrpcStreamingCall call : pendingCalls) {
				try {
					call.onError(throwable);
				}
				catch (Exception e) {
					Log.debug("Exception in invoking onError: " + e);
				}
			}
			pendingCalls.clear();
		}

		abortExecutingCalls(throwable);
	}

	@Override
	public void onCompleted() {
		if (!eventLoop.inEventLoop()) {
			eventLoop.schedule(this::onCompleted, 0, TimeUnit.NANOSECONDS);
			return;
		}

		abortExecutingCalls(new AerospikeException(ResultCode.SERVER_ERROR,
			"stream completed before all responses have been received"));
	}

	LinkedList<GrpcStreamingCall> getPendingCalls() {
		return pendingCalls;
	}

	MethodDescriptor<Kvs.AerospikeRequestPayload,
		Kvs.AerospikeResponsePayload> getMethodDescriptor() {
		return methodDescriptor;
	}

	int getOngoingRequests() {
		return executingCalls.size() + pendingCalls.size();
	}

	public int getId() {
		return id;
	}

	public int getRequestsCompleted() {
		return requestsCompleted;
	}

	int getMaxConcurrentRequests() {
		return maxConcurrentRequests;
	}

	int getTotalRequestsToExecute() {
		return totalRequestsToExecute;
	}

	@Override
	public String toString() {
		return "GrpcStream{id=" + id + ", channelExecutor=" + channelExecutor + '}';
	}

	public int getExecutedRequests() {
		return getRequestsCompleted() + getOngoingRequests();
	}

	public void executePendingCalls() {
		if (isClosed) {
			return;
		}

		if (pendingCalls.isEmpty()) {
			if (streamIdleStartTime == 0) {
				streamIdleStartTime = System.currentTimeMillis();
			}

			if (streamIdleStartTime + IDLE_TIMEOUT <= System.currentTimeMillis() && !streamHalfClosed) {
				streamHalfClosed = true;
				requestObserver.onCompleted();
			}
		}
		else if (streamIdleStartTime != 0) {
			streamIdleStartTime = 0;
		}

		if (streamHalfClosed) {
			if (executingCalls.isEmpty()) {
				// All executing calls are over.
				markClosed();
			}

			// Should not push any new calls on this stream.
			return;
		}

		Iterator<GrpcStreamingCall> iterator = pendingCalls.iterator();
		while (iterator.hasNext()) {
			GrpcStreamingCall call = iterator.next();

			if (call.hasSendDeadlineExpired() || call.hasExpired()) {
				call.onError(new AerospikeException.Timeout(call.getPolicy(),
					call.getIteration()));

				iterator.remove(); // Remove from pending.
			}
			else if (executingCalls.size() < maxConcurrentRequests && requestsSent < totalRequestsToExecute) {
				execute(call);
				iterator.remove(); // Remove from pending.
			}
			else {
				// Call remains in pending.
			}
		}
	}

	private void execute(GrpcStreamingCall call) {
		try {
			if (call.hasExpired()) {
				call.onError(new AerospikeException.Timeout(call.getPolicy(),
					call.getIteration()));
				return;
			}

			Kvs.AerospikeRequestPayload.Builder requestBuilder = call.getRequestBuilder();

			int requestId = requestsSent++;
			requestBuilder
				.setId(requestId)
				.setIteration(call.getIteration());

			GrpcConversions.setRequestPolicy(call.getPolicy(), requestBuilder);
			Kvs.AerospikeRequestPayload requestPayload = requestBuilder
				.build();
			executingCalls.put(requestId, call);

			requestObserver.onNext(requestPayload);

			if (requestsSent >= totalRequestsToExecute) {
				// Complete this stream.
				requestObserver.onCompleted();
				streamHalfClosed = true;
			}

			if (call.hasExpiry()) {
				// TODO: Is there a need for a more efficient implementation in
				//  terms of the call cancellation.
				eventLoop.schedule(() -> onCallExpired(requestId),
					call.nanosTillExpiry(), TimeUnit.NANOSECONDS);
			}
		}
		catch (Exception e) {
			// Failure in scheduling or delegating through request observer.
			call.onError(e);
		}
	}

	private void onCallExpired(int callId) {
		GrpcStreamingCall call = executingCalls.remove(callId);

		// Call has completed.
		if (call == null) {
			return;
		}

		// Cancel call.
		call.onError(new AerospikeException.Timeout(call.getPolicy(), call.getIteration()));

		// Abort long-running calls at server.
		if (!call.isSingleResponse()) {
			abortCallAtServer(call, callId);
		}
	}

	boolean canEnqueue() {
		return !isClosed && !streamHalfClosed && requestsSent < totalRequestsToExecute;
	}

	/**
	 * Enqueue the call to this stream. Should only be invoked if
	 * {@link #canEnqueue()} returned true.
	 */
	void enqueue(GrpcStreamingCall call) {
		pendingCalls.add(call);
	}

	public void closePendingCalls() {
		pendingCalls.forEach(call -> {
			try {
				call.failIfNotComplete(ResultCode.CLIENT_ERROR);
			}
			catch (Exception e) {
				Log.error("Error shutting down " + this.getClass() + ": " + e.getMessage());
			}
		});

		pendingCalls.clear();

		executingCalls.values().forEach(call -> {
			try {
				call.failIfNotComplete(ResultCode.CLIENT_ERROR);
			}
			catch (Exception e) {
				Log.error("Error shutting down " + this.getClass() + ": " + e.getMessage());
			}
		});

		executingCalls.clear();

		// For hygiene complete the stream as well.
		try {
			requestObserver.onCompleted();
			streamHalfClosed =true;
		}
		catch (Throwable t) {
			// Ignore.
		}
	}
}
