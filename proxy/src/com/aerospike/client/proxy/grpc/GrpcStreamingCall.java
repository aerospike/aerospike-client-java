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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.Policy;
import com.aerospike.proxy.client.Kvs;

import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

/**
 * A gRPC call that is converted to a streaming call for performance.
 */
public class GrpcStreamingCall {
    /**
     * The streaming method to execute for this unary call.
     */
    private final MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor;

	/**
	 * The request builder populated with command call specific parameters.
	 */
	private final Kvs.AerospikeRequestPayload.Builder requestBuilder;

	/**
	 * The stream response observer for the call.
	 */
	private final StreamObserver<Kvs.AerospikeResponsePayload> responseObserver;

	/**
	 * The deadline in nanoseconds w.r.t System.nanoTime() for this call to
	 * complete. A value of zero indicates that the call has no deadline.
	 */
	private final long deadlineNanos;

	/**
	 * The deadline in nanoseconds w.r.t System.nanoTime() for this call to
	 * be handed to the underlying gRPC sub system.
	 */
	private final long sendDeadlineNanos;

	/**
	 * Aerospike client policy for this request.
	 */
	private final Policy policy;

	/**
	 * Iteration number of this request.
	 */
	private final int iteration;

	/**
	 * Does this call have single or multiple responses from the server? Calls
	 * like batch, query, scan have multiple responses from the server.
	 */
	private final boolean isSingleResponse;

	/**
	 * Indicates if this call completed (successfully or unsuccessfully).
	 */
	private volatile boolean completed;

	/**
	 * Indicates if this call aborted due to an application exception..
	 */
	private volatile boolean aborted;

	protected GrpcStreamingCall(GrpcStreamingCall other) {
		this(other.methodDescriptor, other.requestBuilder, other.getPolicy(),
			other.iteration, other.deadlineNanos, other.sendDeadlineNanos,
			other.isSingleResponse, other.responseObserver);

		completed = other.completed;
		aborted = other.aborted;
	}

	public GrpcStreamingCall(
		MethodDescriptor<Kvs.AerospikeRequestPayload,
			Kvs.AerospikeResponsePayload> methodDescriptor,
		Kvs.AerospikeRequestPayload.Builder requestBuilder,
		Policy policy,
		int iteration,
		long deadlineNanos,
		long sendDeadlineNanos,
		boolean isSingleResponse,
		StreamObserver<Kvs.AerospikeResponsePayload> responseObserver
	) {
		this.responseObserver = responseObserver;
		this.methodDescriptor = methodDescriptor;
		this.requestBuilder = requestBuilder;
		this.iteration = iteration;
		this.policy = policy;
		this.deadlineNanos = deadlineNanos;
		this.sendDeadlineNanos = sendDeadlineNanos;
		this.isSingleResponse = isSingleResponse;
	}

	public void onNext(Kvs.AerospikeResponsePayload payload) {
		responseObserver.onNext(payload);

		if (!payload.getHasNext()) {
			completed = true;
			responseObserver.onCompleted();
		}
	}

	public void onError(Throwable t) {
		completed = true;
		responseObserver.onError(t);
	}

	/**
	 * Fail the call if it is not completed.
	 *
	 * @param resultCode aerospike error code.
	 */
	public void failIfNotComplete(int resultCode) {
		if (!hasCompleted()) {
			onError(new AerospikeException(resultCode));
		}
	}

	/**
	 * Fail the call if it is not completed.
	 *
	 * @param throwable cause of failure.
	 */
	public void failIfNotComplete(Throwable throwable) {
		if (!hasCompleted()) {
			onError(throwable);
		}
	}

	/**
	 * @return <code>true</code> if this call has completed either because
	 * {@link #onNext(Kvs.AerospikeResponsePayload)} or
	 * {@link #onError(Throwable)} was invoked.
	 */
	public boolean hasCompleted() {
		return completed;
	}

	public MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> getStreamingMethodDescriptor() {
		return methodDescriptor;
	}

	/**
	 * @return true if this call has expired.
	 */
	public boolean hasExpired() {
		return hasExpiry() && (System.nanoTime() - deadlineNanos) >= 0;
	}

	/**
	 * @return true if the send deadline has expired.
	 */
	public boolean hasSendDeadlineExpired() {
		return (System.nanoTime() - sendDeadlineNanos) >= 0;
	}

	public boolean hasExpiry() {
		return deadlineNanos != 0;
	}

	public long nanosTillExpiry() {
		if (!hasExpiry()) {
			throw new IllegalStateException("call does not expire");
		}
		long nanosTillExpiry = deadlineNanos - System.nanoTime();
		return nanosTillExpiry > 0 ? nanosTillExpiry : 0;
	}

	public Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
		return requestBuilder;
	}

	public int getIteration() {
		return iteration;
	}

	public Policy getPolicy() {
		return policy;
	}

	public void markAborted() {
		this.aborted = true;
		this.completed = true;
	}

	public boolean isAborted() {
		return aborted;
	}

	public boolean isSingleResponse() {
		return isSingleResponse;
	}
}
