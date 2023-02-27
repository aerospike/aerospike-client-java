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

import java.util.concurrent.TimeUnit;

import com.aerospike.client.policy.Policy;
import com.aerospike.proxy.client.Kvs;
import com.google.protobuf.ByteString;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

/**
 * A unary gRPC call that is converted to a streaming call for performance.
 */
public class GrpcStreamingUnaryCall {
	/**
	 * The streaming method to execute for this unary call.
	 */
	private final MethodDescriptor<Kvs.AerospikeRequestPayload,
			Kvs.AerospikeResponsePayload> streamingMethodDescriptor;

	/**
	 * The request payload.
	 */
	private final ByteString requestPayload;

	/**
	 * The stream response observer for the call.
	 */
	private final StreamObserver<Kvs.AerospikeResponsePayload> responseObserver;

	/**
	 * The deadline in nanoseconds w.r.t System.nanoTime().
	 */
	private final long expiresAtNanos;

	/**
	 * Aerospike client policy for this request.
	 */
	private final Policy policy;

	/**
	 * Iteration number of this request.
	 */
	private final int iteration;

	/**
	 * Indicates if this call completed (successfully or unsuccessfully).
	 */
	private volatile boolean completed;

	protected GrpcStreamingUnaryCall(GrpcStreamingUnaryCall other) {
		this(other.streamingMethodDescriptor, other.requestPayload, other.getPolicy(),
				other.iteration, other.responseObserver);
		completed = other.completed;
	}


	public GrpcStreamingUnaryCall(MethodDescriptor<Kvs.AerospikeRequestPayload,
			Kvs.AerospikeResponsePayload> streamingMethodDescriptor,
								  ByteString requestPayload,
								  Policy policy,
								  int iteration,
								  StreamObserver<Kvs.AerospikeResponsePayload>
										  responseObserver) {
		this.responseObserver = responseObserver;
		this.streamingMethodDescriptor = streamingMethodDescriptor;
		this.requestPayload = requestPayload;
		this.iteration = iteration;
		this.policy = policy;

		if (policy.totalTimeout > 0) {
			this.expiresAtNanos =
					System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(policy.totalTimeout);
		}
		else {
			// TODO: should 0 (no timeout) be allowed?
			this.expiresAtNanos =
					System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
		}
	}

	public void onSuccess(Kvs.AerospikeResponsePayload payload) {
		completed = true;
		responseObserver.onNext(payload);
		responseObserver.onCompleted();
	}

	public void onError(Throwable t) {
		completed = true;
		responseObserver.onError(t);
	}

	/**
	 * @return <code>true</code> iff this call has completed either because
	 * {@link #onSuccess(Kvs.AerospikeResponsePayload)} or
	 * {@link #onError(Throwable)} was invoked.
	 */
	public boolean hasCompleted() {
		return completed;
	}

	public MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> getStreamingMethodDescriptor() {
		return streamingMethodDescriptor;
	}

	/**
	 * @return true if this call has expired.
	 */
	public boolean hasExpired() {
		return hasExpiry() && System.nanoTime() >= expiresAtNanos;
	}

	public boolean hasExpiry() {
		return expiresAtNanos > 0;
	}

	public long nanosTillExpiry() {
		if (expiresAtNanos == 0) {
			return Long.MAX_VALUE;
		}
		long nanosTillExpiry = expiresAtNanos - System.nanoTime();
		return nanosTillExpiry > 0 ? nanosTillExpiry : 0;
	}

	public ByteString getRequestPayload() {
		return requestPayload;
	}

	public int getIteration() {
		return iteration;
	}

	public Policy getPolicy() {
		return policy;
	}
}
