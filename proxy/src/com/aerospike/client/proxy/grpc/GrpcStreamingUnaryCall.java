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
import com.aerospike.proxy.client.Kvs;

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
    private final byte[] requestPayload;

    /**
     * The total call timeout.
     */
    private final long timeout;

    /**
     * The stream response observer for the call.
     */
    private final StreamObserver<Kvs.AerospikeResponsePayload> responseObserver;

    /**
     * Indicates if this call has been completed successfully or otherwise.
     */
    private volatile boolean isComplete = false;

    public GrpcStreamingUnaryCall(MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor, byte[] requestPayload, long timeout, StreamObserver<Kvs.AerospikeResponsePayload> responseObserver) {
        this.responseObserver = responseObserver;
        this.methodDescriptor = methodDescriptor;
        this.requestPayload = requestPayload;
        this.timeout = timeout;
    }

    public void onSuccess(Kvs.AerospikeResponsePayload payload) {
        try {
            responseObserver.onNext(payload);
            responseObserver.onCompleted();
        } finally {
            setComplete(true);
        }
    }

    public void onError(AerospikeException e) {
        try {
            responseObserver.onError(e);
        } finally {
            setComplete(true);
        }
    }

    public MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> getMethodDescriptor() {
        return methodDescriptor;
    }

    public byte[] getRequestPayload() {
        return requestPayload;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isComplete() {
        return isComplete;
    }

    private void setComplete(boolean complete) {
        isComplete = complete;
    }
}
