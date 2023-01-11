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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;

/**
 * Executes gRPC calls.
 */
class GrpcCallQueue {
    private static final int QUEUES_PER_CALL = Runtime.getRuntime().availableProcessors();
    /**
     * Maximum allowed queue size.
     * <p>
     * TODO: Arbitrary high number need to derive from client policy.
     */
    private final int maxQueueSize = 100000;

    private final AtomicInteger totalQueueSize = new AtomicInteger(0);
    /**
     * Per method call queues.
     */
    private final ConcurrentHashMap<String,
            QueueList> perMethodCallQueues;
    /**
     * Index into streaming call to poll from.
     */
    private int callPollIndex = 0;

    private static class QueueList {
        int pollIndexEnqueue = 0;
        int pollIndexDequeue = 0;

        @SuppressWarnings("unchecked")
        ConcurrentLinkedQueue<GrpcStreamingUnaryCall>[] queues = new ConcurrentLinkedQueue[QUEUES_PER_CALL];

        QueueList() {
            for (int i = 0; i < queues.length; i++) {
                queues[i] = new ConcurrentLinkedQueue<>();
            }
        }

        ConcurrentLinkedQueue<GrpcStreamingUnaryCall> getRandomQueueEnqueue() {
            return queues[Math.abs(pollIndexEnqueue++ % QUEUES_PER_CALL)];
        }

        ConcurrentLinkedQueue<GrpcStreamingUnaryCall> getRandomQueueDequeue() {
            return queues[Math.abs(pollIndexDequeue++ % QUEUES_PER_CALL)];
        }

    }

    GrpcCallQueue() {
        perMethodCallQueues =
                new ConcurrentHashMap<>(GrpcCallExecutor.STREAMING_CALLS.size());
        for (String method : GrpcCallExecutor.STREAMING_CALLS) {
            perMethodCallQueues.put(method, new QueueList());
        }
    }


    /**
     * Enqueue a new call for execution.
     *
     * @param call the unary grpc call to enqueue.
     * @throws AerospikeException if the request cannot be enqueued.
     */
    public void enqueue(GrpcStreamingUnaryCall call) throws AerospikeException {
        if (totalQueueSize.get() > maxQueueSize) {
            throw new AerospikeException(ResultCode.NO_MORE_CONNECTIONS,
                    "Maximum queue " + maxQueueSize +
                            " size exceeded");
        }
        ConcurrentLinkedQueue<GrpcStreamingUnaryCall> currentQueue =
                perMethodCallQueues.get(call.getMethodDescriptor().getFullMethodName()).getRandomQueueEnqueue();
        currentQueue.add(call);

        totalQueueSize.incrementAndGet();
    }

    /**
     * Dequeue a batch of pending calls. All calls are guaranteed to be for
     * the same streaming method.
     *
     * @param maxBatchSize  the maximum number of calls to batch.
     * @param maxBatchBytes the maximum payload sizes to batch.
     * @return a call from the queue if it exists, else null.
     */
    public List<GrpcStreamingUnaryCall> dequeueBatch(int maxBatchSize,
                                                     int maxBatchBytes) {
        int totalCallsDescriptors = GrpcCallExecutor.STREAMING_CALLS.size();
        int startIndex = Math.abs(callPollIndex++) % totalCallsDescriptors;
        List<GrpcStreamingUnaryCall> batch = new ArrayList<>(maxBatchSize);
        for (int i = 0; i < GrpcCallExecutor.STREAMING_CALLS.size(); i++) {
            String nextCallName = GrpcCallExecutor.STREAMING_CALLS.get((i + startIndex) % totalCallsDescriptors);
            ConcurrentLinkedQueue<GrpcStreamingUnaryCall> currentQueue =
                    perMethodCallQueues.get(nextCallName).getRandomQueueDequeue();
            GrpcStreamingUnaryCall firstCall = currentQueue.poll();
            if (firstCall != null) {
                // We found a non-empty call queue. Return a batch from here.
                int batchSize = 1;
                int batchBytes = firstCall.getRequestPayload().length;
                batch.add(firstCall);

                while (batchSize <= maxBatchSize && batchBytes <= maxBatchBytes) {
                    GrpcStreamingUnaryCall nextCall = currentQueue.poll();
                    if (nextCall == null) {
                        break;
                    }
                    batch.add(nextCall);
                    batchSize++;
                    batchBytes += nextCall.getRequestPayload().length;
                }
                break;
            }
        }
        totalQueueSize.addAndGet(-batch.size());
        return batch;
    }
}
