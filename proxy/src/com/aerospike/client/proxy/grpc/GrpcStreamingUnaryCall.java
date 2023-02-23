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
            Kvs.AerospikeResponsePayload> methodDescriptor;

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

    protected GrpcStreamingUnaryCall(GrpcStreamingUnaryCall other) {
        this(other.methodDescriptor, other.requestPayload, other.getPolicy(),
                other.iteration, other.expiresAtNanos, other.responseObserver);
    }


    public GrpcStreamingUnaryCall(MethodDescriptor<Kvs.AerospikeRequestPayload,
            Kvs.AerospikeResponsePayload> methodDescriptor,
                                  ByteString requestPayload,
                                  Policy policy,
                                  int iteration,
                                  long expiresAtNanos,
                                  StreamObserver<Kvs.AerospikeResponsePayload>
                                          responseObserver) {
        this.responseObserver = responseObserver;
        this.methodDescriptor = methodDescriptor;
        this.requestPayload = requestPayload;
        this.iteration = iteration;
        this.policy = policy;
        this.expiresAtNanos = expiresAtNanos;
    }

    public void onSuccess(Kvs.AerospikeResponsePayload payload) {
        responseObserver.onNext(payload);
        responseObserver.onCompleted();
    }

    public void onError(Throwable t) {
        responseObserver.onError(t);
    }

    public MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> getMethodDescriptor() {
        return methodDescriptor;
    }

    /**
     * @return true if this call has expired.
     */
    public boolean hasExpired() {
        return policy.totalTimeout > 0 && System.nanoTime() - expiresAtNanos >= 0;
    }

    public boolean hasExpiry() {
        return policy.totalTimeout > 0;
    }

    public long nanosTillExpiry() {
        if (policy.totalTimeout == 0) {
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
