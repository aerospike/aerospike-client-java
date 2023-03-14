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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jctools.queues.SpscUnboundedArrayQueue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.proxy.client.Kvs;
import com.google.protobuf.ByteString;

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
 *
 * <p><em>NOTE</em> All methods of the stream are executed within a single
 * thread. This is implemented by
 * <ul>
 *     <li>having the channel configured to use the direct executor</li>
 *     <li>have the channel and streams associated with the channel be
 *     executed on a single event loop</li>
 * </ul>
 *
 * <p>TODO: Should the stream be closed if it has been idle for some duration?
 */
public class GrpcStream implements StreamObserver<Kvs.AerospikeResponsePayload>, Closeable {
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
	 * The gRPC client policy.
	 */
	private final GrpcClientPolicy grpcClientPolicy;
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
	 */
	private final SpscUnboundedArrayQueue<GrpcStreamingCall> pendingCalls;

	/**
	 * Map of request id to the calls executing in this stream.
	 */
	private final Map<Integer, GrpcStreamingCall> executingCalls =
		new HashMap<>();

	/**
	 * Is the stream closed. This variable is only accessed from the event
	 * loop thread assigned to this stream and its channel.
	 */
	private boolean isClosed = false;

	// Stream statistics. These are only accessed from the event loop thread
	// assigned to this stream and its channel.
	private long bytesSent;
	private long bytesReceived;
	private int requestsSent;
	private int responsesReceived;
	private int requestsInFlight;

	public GrpcStream(GrpcChannelExecutor channelExecutor,
					  MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor,
					  SpscUnboundedArrayQueue<GrpcStreamingCall> pendingCalls,
					  CallOptions callOptions,
					  GrpcClientPolicy grpcClientPolicy,
					  int streamIndex, EventLoop eventLoop) {
		this.channelExecutor = channelExecutor;
		this.methodDescriptor = methodDescriptor;
		this.pendingCalls = pendingCalls;
		this.grpcClientPolicy = grpcClientPolicy;
		this.id = streamIndex;
		this.eventLoop = eventLoop;
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
		int id = aerospikeResponsePayload.getId();
		GrpcStreamingCall call;

		int payloadSize = aerospikeResponsePayload.getPayload().size();
		bytesReceived += payloadSize;
		channelExecutor.onPayloadReceived(payloadSize);

		if (aerospikeResponsePayload.getHasNext()) {
			call = executingCalls.get(id);
		}
		else {
			call = executingCalls.remove(id);

			// Update stats.
			requestsInFlight--;
			responsesReceived++;
			channelExecutor.onRequestCompleted();
		}

		// Call might have expired and been cancelled.
		if (call != null) {
			call.onSuccess(aerospikeResponsePayload);
		}

		// TODO can it ever be greater than?
		if (responsesReceived >= grpcClientPolicy.totalRequestsPerStream) {
			// Complete this stream.
			requestObserver.onCompleted();
		}
		else {
			executeCall();
		}
	}

	private void abortExecutingCalls(Throwable throwable) {
		isClosed = true;

		for (GrpcStreamingCall call : executingCalls.values()) {
			call.onError(throwable);
			requestsInFlight--;
		}

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

		abortExecutingCalls(throwable);
	}

	@Override
	public void onCompleted() {
		abortExecutingCalls(new AerospikeException(ResultCode.SERVER_ERROR,
			"stream completed before all responses have been received"));
	}

	SpscUnboundedArrayQueue<GrpcStreamingCall> getQueue() {
		return pendingCalls;
	}

	MethodDescriptor<Kvs.AerospikeRequestPayload,
		Kvs.AerospikeResponsePayload> getMethodDescriptor() {
		return methodDescriptor;
	}

	int getOngoingRequests() {
		return requestsInFlight + pendingCalls.size();
	}

	public int getId() {
		return id;
	}

	public long getBytesSent() {
		return bytesSent;
	}

	public long getBytesReceived() {
		return bytesReceived;
	}

	public int getRequestsSent() {
		return requestsSent;
	}

	public int getResponsesReceived() {
		return responsesReceived;
	}

	@Override
	public String toString() {
		return "GrpcStream{id=" + id + ", channelExecutor=" + channelExecutor + '}';
	}

	public int getTotalExecutedRequests() {
		return getRequestsSent() + getOngoingRequests();
	}

	public void executeCall() {
		if (isClosed) {
			return;
		}

		pendingCalls.drain(GrpcStream.this::execute, idleCounter -> idleCounter,
			() -> !pendingCalls.isEmpty() &&
				requestsSent < grpcClientPolicy.totalRequestsPerStream &&
				requestsInFlight < grpcClientPolicy.maxConcurrentRequestsPerStream);
	}


	private void execute(GrpcStreamingCall call) {
		try {
			if (call.hasExpired()) {
				call.onError(new AerospikeException.Timeout(call.getPolicy(),
					call.getIteration()));
				return;
			}

			ByteString payload = call.getRequestPayload();

			// Update stats.
			requestsInFlight++;
			bytesSent += payload.size();

			int requestId = requestsSent++;
			Kvs.AerospikeRequestPayload.Builder requestBuilder = Kvs.AerospikeRequestPayload.newBuilder()
				.setPayload(payload)
				.setId(requestId)
				.setIteration(call.getIteration());

			GrpcConversions.setRequestPolicy(call.getPolicy(), requestBuilder);
			Kvs.AerospikeRequestPayload requestPayload = requestBuilder
				.build();
			executingCalls.put(requestId, call);

			requestObserver.onNext(requestPayload);

			if (call.hasExpiry()) {
				// TODO: Is there a need for a more efficient implementation in
				//  terms of the call cancellation.
				eventLoop.schedule(() -> onCancelCall(requestId),
					call.nanosTillExpiry(), TimeUnit.NANOSECONDS);
			}
		}
		catch (Exception e) {
			// Failure in scheduling or delegating through request observer.
			call.onError(e);
		}
	}

	private void onCancelCall(int callId) {
		GrpcStreamingCall call = executingCalls.remove(callId);

		// Call might have completed.
		if (call != null) {
			call.onError(new AerospikeException.Timeout(call.getPolicy(),
				call.getIteration()));
		}
	}

	void enqueue(GrpcStreamingCall call) {
		// TODO: can this call fail?
		pendingCalls.add(call);
	}

	@Override
	public void close() throws IOException {
		while (!pendingCalls.isEmpty()) {
			try {
				pendingCalls.drain(call -> call.failIfNotComplete(ResultCode.CLIENT_ERROR));
			}
			catch (Exception e) {
				Log.error("Error shutting down " + this.getClass() + ": " + e.getMessage());
			}
		}
		executingCalls.values().forEach(call -> {
				try {
					call.failIfNotComplete(ResultCode.CLIENT_ERROR);
				}
				catch (Exception e) {
					Log.error("Error shutting down " + this.getClass() + ": " + e.getMessage());
				}
			}
		);
	}
}
