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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.proxy.auth.AuthTokenManager;
import com.aerospike.proxy.client.Kvs;
import com.google.protobuf.ByteString;

import io.grpc.CallOptions;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

/**
 * An executor that executes calls over a single underlying gRPC managed
 * channel.
 */
public class GrpcChannelExecutor {
    private final ManagedChannel channel;

    private final Worker[] workers;

    /**
     * Maximum number of calls to batch in a single streaming call.
     */
    private final int maxBatchSize = 100;

    /**
     * Maximum number bytes to to batch in a single streaming call.
     */
    private final int maxBatchBytes = 1024 * 1024;

    /**
     * Maximum allowed duration for a streaming call.
     */
    private final long maxStreamCallDuration = 1000;

    /**
     * The work queue.
     */
    private final GrpcCallQueue queue;

    /**
     * The token manager for making requests.
     */
    private final AuthTokenManager tokenManager;


    public GrpcChannelExecutor(ManagedChannel channel,
                               int maxConcurrentStreams, GrpcCallQueue queue, AuthTokenManager tokenManager) {
        this.channel = channel;
        this.queue = queue;
        this.tokenManager = tokenManager;
        this.workers = new Worker[maxConcurrentStreams];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    void poke() {
        for (Worker worker : workers) {
            worker.poke();
        }
    }

    /**
     * Executes call in a continuous loop. The actual call execution is in the
     * channel
     * threads. This class schedules calls.
     */
    private class Worker {
        AtomicBoolean isBusy = new AtomicBoolean(false);

        Worker() {
            consume();
        }

        /**
         * Pke the worker to start consuming if it is idle.
         */
        void poke() {
            if (!isBusy.get()) {
                consume();
            }

            // TODO: Do this if connection is idle for too long.
            if (channel.getState(false) == ConnectivityState.IDLE) {
                channel.getState(true);
            }
        }

        /**
         * Start call execution.
         */
        private void consume() {
            if (isBusy.get() || ((channel.getState(false) != ConnectivityState.READY))) {
                // worker is busy of the channel is not established.
                return;
            }
            List<GrpcStreamingUnaryCall> batch = queue.dequeueBatch(maxBatchSize, maxBatchBytes);
            if (!batch.isEmpty()) {
                try {
                    isBusy.set(true);
                    dispatchBatch(batch);
                } catch (Exception e) {
                    // TODO: create correct result coe.
                    completeBatch(new AerospikeException(e), batch);
                }
            }
        }

        private void completeBatch(AerospikeException pendingCallError,
                                   List<GrpcStreamingUnaryCall> batch) {
            // TODO: Convert to Aerospike exception.
            errorPending(pendingCallError, batch);
            // Mark as not busy.
            isBusy.set(false);
            // start new consumption cycle.
            consume();
        }

        /**
         * Send errors to pending calls.
         */
        private void errorPending(AerospikeException e,
                                  List<GrpcStreamingUnaryCall> batch) {
            for (GrpcStreamingUnaryCall call : batch) {
                if (!call.isComplete()) {
                    try {
                        call.onError(e);
                    } catch (Exception unexpected) {
                        if (Log.warnEnabled()) {
                            Log.warn("Error invoking success " +
                                    "handler " + unexpected);
                        }
                    }
                }
            }
        }

        private void dispatchBatch(List<GrpcStreamingUnaryCall> batch) {
            //System.out.println("Batch Size " + batch.size());
            MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> descriptor = batch.get(0).getMethodDescriptor();
            CallOptions callOptions = CallOptions.DEFAULT;
            // TODO: set deadline for overall call, but first ensure the
            //  channel connection is active.
            long totalTimeout = 0;
            for (GrpcStreamingUnaryCall call : batch) {
                if (call.getTimeout() > 0) {
                    totalTimeout += call.getTimeout();
                }
            }

            /*
            // TODO: Set deadline.
            if (totalTimeout > 0) {
                callOptions =
                        callOptions.withDeadline(Deadline.after(totalTimeout,
                                TimeUnit.MILLISECONDS));
            }*/
            if (tokenManager != null) {
                callOptions = tokenManager.setCallCredentials(callOptions);
            }

            AerospikeResponsePayloadStreamObserver responseObserver = new AerospikeResponsePayloadStreamObserver();
            StreamObserver<Kvs.AerospikeRequestPayload> requestObserver =
                    ClientCalls.asyncBidiStreamingCall(channel.newCall(descriptor, callOptions), responseObserver
                    );
            responseObserver.setRequestObserver(requestObserver);
            responseObserver.streamBatch(batch);
        }

        private class AerospikeResponsePayloadStreamObserver implements StreamObserver<Kvs.AerospikeResponsePayload> {
            private List<GrpcStreamingUnaryCall> batch;
            private StreamObserver<Kvs.AerospikeRequestPayload> requestObserver;

            /**
             * Request ID for the next call that will be made.
             */
            private int nextRequestID = 0;

            /**
             * Request ID for files element in the batch.
             */
            private int batchRequestIdStart = 0;

            /**
             * Total call sent on this stream.
             */
            private int streamedSize = 0;

            /**
             * Total bytes sent on this stream.
             */
            private int streamedBytes = 0;

            private final long streamStartTime;

            public AerospikeResponsePayloadStreamObserver() {
                streamStartTime = System.currentTimeMillis();
            }

            public void setRequestObserver(StreamObserver<Kvs.AerospikeRequestPayload> requestObserver) {
                this.requestObserver = requestObserver;
            }

            @Override
            public void onNext(Kvs.AerospikeResponsePayload value) {
                int callId = value.getId();
                if (callId < batchRequestIdStart
                        || callId >= (batch.size() + batchRequestIdStart)) {
                    // Something went wrong.
                    if (Log.warnEnabled()) {
                        Log.warn("Received invalid batchID " + callId);
                    }
                    requestObserver.onError(new Exception("Received invalid batchID " + callId));
                    return;
                }

                try {
                    batch.get(callId - batchRequestIdStart).onSuccess(value);
                } catch (Exception e) {
                    if (Log.warnEnabled()) {
                        Log.warn("Error invoking success " +
                                "handler " + e);
                    }
                }

                boolean allCompleted = true;
                for (GrpcStreamingUnaryCall grpcStreamingUnaryCall : batch) {
                    allCompleted &= grpcStreamingUnaryCall.isComplete();
                }

                if (allCompleted) {
                    long elapsedTime =
                            System.currentTimeMillis() - streamStartTime;
                    if (streamedSize < maxBatchSize
                            && streamedBytes < maxBatchBytes
                            && elapsedTime < maxStreamCallDuration) {
                        List<GrpcStreamingUnaryCall> nextBatch = queue.dequeueBatch(maxBatchSize - streamedSize,
                                maxBatchBytes - streamedBytes);
                        if (!nextBatch.isEmpty()) {
                            streamBatch(nextBatch);
                        } else {
                            requestObserver.onCompleted();
                        }
                    } else {
                        requestObserver.onCompleted();
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                // TODO: convert to correct Aerospike exception.
                completeBatch(new AerospikeException(t), batch);
            }


            @Override
            public void onCompleted() {
                completeBatch(new AerospikeException(ResultCode.CLIENT_ERROR, "streaming call terminated without response"), batch);
            }

            public void streamBatch(List<GrpcStreamingUnaryCall> batch) {
                this.batch = batch;
                try {
                    batchRequestIdStart = nextRequestID;
                    for (GrpcStreamingUnaryCall call : batch) {
                        streamedSize++;
                        streamedBytes += call.getRequestPayload().length;
                        // TODD pass iteration.
                        requestObserver.onNext(Kvs.AerospikeRequestPayload.newBuilder()
                        		// TODO: Use ByteString.copyFrom(byte[], off, len);
                                .setPayload(ByteString.copyFrom(call.getRequestPayload()))
                                .setId(nextRequestID++).build());
                    }
                } catch (Throwable t) {
                    requestObserver.onError(t);
                    throw t;
                }
            }
        }
    }
}





