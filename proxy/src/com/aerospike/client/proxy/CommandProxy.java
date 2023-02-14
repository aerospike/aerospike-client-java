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
package com.aerospike.client.proxy;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcStreamingUnaryCall;
import com.aerospike.client.util.Util;
import com.aerospike.proxy.client.KVSGrpc;
import com.aerospike.proxy.client.Kvs;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public abstract class CommandProxy {
    /**
     * The gRPC channel pool.
     */
    protected final GrpcCallExecutor executor;
    /**
     * The command execution policy.
     */
    protected final Policy policy;
    /**
     * TODO: Remove this?
     * The event loop that will run the application callback.
     */
    @Nullable
    private final EventLoop eventLoop;
    /**
     * The iteration number of the command execution.
     */
    private volatile int iteration;
    /**
     * The reason for the execution failure on the previous iteration.
     */
    private AerospikeException exception;
    /**
     * Indicates whether the client timed on the previous execution iteration.
     */
    private boolean isClientTimeout;
    /**
     * The number of times the command was sent on the wire.
     */
    private int commandSentCounter;
    /**
     * Absolute deadline for this command to complete across all iterations
     * from the start of execution.
     */
    private long deadlineNanos;

    public CommandProxy(GrpcCallExecutor executor, Policy policy) {
        this.executor = executor;
        this.policy = policy;
        this.eventLoop = null;
    }

    /**
     * Subclasses should call this method on successful completion of
     * server call.
     */
    void onSuccess() {
        // TODO: do some bookkeeping.
    }

    /**
     * Subclasses should call this method on failed completion of
     * server call.
     *
     * @param throwable       reason for the failure.
     * @param responsePayload response from the proxy server.
     */
    void onFailure(Throwable throwable,
                   @Nullable Kvs.AerospikeResponsePayload responsePayload) {
        setException(throwable, responsePayload);

        // TODO: should this be scheduled on another thread.
        executeCommand();
    }

    public void execute() {
        //TODO: backoff based on in-flight command count.
        if (policy.totalTimeout > 0) {
            deadlineNanos =
                    System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(policy.totalTimeout);
        }

        executeCommand();
    }

    protected void executeCallback(Runnable callback) {
    	// TODO: Re-evalute if redirecting to another eventloop thread makes sense.
        if (eventLoop == null || eventLoop.inEventLoop()) {
            callback.run();
        } else {
            eventLoop.execute(callback);
        }
    }

    private void executeCommand() {
        boolean isRetry = iteration > 0;
        if (isRetry && !isClientTimeout &&
                policy.sleepBetweenRetries > 0 &&
                System.nanoTime() < deadlineNanos) {
            schedule(this::executeSendRequest, policy.sleepBetweenRetries,
                    TimeUnit.MILLISECONDS);
        } else {
            executeSendRequest();
        }
    }


    protected void schedule(Runnable command, long delay,
                            TimeUnit timeUnit) {
        executor.getEventLoop().schedule(command, delay, timeUnit);
    }

    private void executeSendRequest() {
        Command command = new Command(policy.socketTimeout, policy.totalTimeout, policy.maxRetries);
    	writeCommand(command);

    	ByteString payload = ByteString.copyFrom(command.dataBuffer, 0, command.dataOffset);

        while (iteration <= policy.maxRetries &&
                !shouldHaltOnException() &&
                (System.nanoTime() < deadlineNanos)) {
            // iteration=0 is initial attempt, iteration=1 is first retry.
            iteration++;
            try {
                sendRequest(payload);
                return;
            } catch (Throwable t) {
                setException(t, null);
            }
        }

        // All attempts have failed.
        if (exception == null && System.nanoTime() > deadlineNanos) {
            // TODO: is iteration number correct?
            exception = new AerospikeException.Timeout(policy, iteration);
        }
        onFailure(exception);
    }

    private void sendRequest(ByteString payload) {
        executor.execute(new GrpcStreamingUnaryCall(KVSGrpc.getPutStreamingMethod(),
                payload, policy, getIteration(),
                new StreamObserver<Kvs.AerospikeResponsePayload>() {
                    @Override
                    public void onNext(Kvs.AerospikeResponsePayload value) {
                        try {
                            receivePayload(value.getPayload());
                        } catch (Throwable t) {
                            onFailure(t, value);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        onFailure(t, null);
                    }

                    @Override
                    public void onCompleted() {
                    }
                }));

        // TODO: this should be incremented only when the request has been
        //  put on the wire?
        incrementCommandSentCounter();
    }

	private void receivePayload(ByteString response) {
		byte[] bytes = response.toByteArray();
		Parser parser = new Parser(bytes);
		parser.parseProto();
		parseResult(parser);
	}

    protected int getIteration() {
        return iteration;
    }

    protected boolean isWrite() {
        return false;
    }

    protected void incrementCommandSentCounter() {
        commandSentCounter++;
    }

    private boolean shouldHaltOnException() {
        // TODO: the command should not be retried on some exceptions.
        return false;
    }

    private void setException(Throwable throwable,
                              @Nullable Kvs.AerospikeResponsePayload responsePayload) {
        // AerospikeException received on the proxy server are transmitted as-is
        // to the proxy client and parsed from the aerospike wire payload to a
        // AerospikeException in the `AbstractCommand.onNext` call . Any server
        // response successfully received by the proxy client should always be
        // an instance of AerospikeException.

        if (throwable instanceof AerospikeException) {
            isClientTimeout = false;
            AerospikeException ae = (AerospikeException) throwable;
            if (ae.getResultCode() == ResultCode.TIMEOUT) {
                exception = new AerospikeException.Timeout(policy, false);
            } else if (ae.getResultCode() == ResultCode.DEVICE_OVERLOAD) {
                exception = ae;
            } else {
                exception = ae;
            }
        } else if (throwable instanceof StatusRuntimeException) {
            exception = toAerospikeException((StatusRuntimeException) throwable);

            // TODO: should more cases be handled for isClientTimeout?
            isClientTimeout = exception instanceof AerospikeException.Timeout;
        } else {
            // TODO: can code fall through to this else branch? Should we
            //  handle more types of exceptions?
            // TODO: Is it correct to blanket convert to CLIENT_ERROR?
            exception = new AerospikeException(ResultCode.CLIENT_ERROR,
                    throwable);
            isClientTimeout = false;
        }

        exception.setIteration(iteration);
        exception.setPolicy(policy);

        //noinspection SimplifiableConditionalExpression
        boolean inDoubt = responsePayload != null ? responsePayload.getInDoubt() : false;
        if (inDoubt) {
            exception.setInDoubt(true);
        } else {
            exception.setInDoubt(isWrite(), commandSentCounter);
        }
    }

    private AerospikeException toAerospikeException(StatusRuntimeException exception) {
        Status.Code code = exception.getStatus().getCode();
        // TODO: check if the gRPC status code to AerospikeException
        //  mappings are correct?
        switch (code) {
            case CANCELLED:
            case UNKNOWN:
            case NOT_FOUND:
            case ALREADY_EXISTS:
            case FAILED_PRECONDITION:
            case ABORTED:
            case OUT_OF_RANGE:
            case UNIMPLEMENTED:
            case INTERNAL:
            case DATA_LOSS:
                return new AerospikeException(ResultCode.CLIENT_ERROR,
                        "gRPC status code=" + code, exception);
            case INVALID_ARGUMENT:
                return new AerospikeException(ResultCode.SERIALIZE_ERROR,
                        exception);
            case DEADLINE_EXCEEDED:
                return new AerospikeException.Timeout(policy, iteration);
            case PERMISSION_DENIED:
                return new AerospikeException(ResultCode.FAIL_FORBIDDEN,
                        exception);
            case RESOURCE_EXHAUSTED:
                return new AerospikeException(ResultCode.QUOTA_EXCEEDED,
                        exception);
            case UNAVAILABLE:
                return new AerospikeException.Backoff(ResultCode.MAX_ERROR_RATE);
            case UNAUTHENTICATED:
                return new AerospikeException(ResultCode.NOT_AUTHENTICATED);
            case OK:
            default:
                return new AerospikeException("gRPC code " + code, exception);
        }
    }

    static void logOnSuccessError(Throwable t) {
		Log.error("onSuccess() error: " + Util.getStackTrace(t));
	}

	abstract void writeCommand(Command command);
    abstract void parseResult(Parser parser);
    abstract void onFailure(AerospikeException ae);
}
