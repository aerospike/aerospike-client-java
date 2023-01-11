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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;

public abstract class AbstractCommand {
    /**
     * The command execution policy.
     */
    protected final Policy policy;
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

    public AbstractCommand(Policy policy) {
        this.policy = policy;
    }

    /**
     * Send the request to the server.
     */
    abstract void sendRequest();

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
     * @param throwable reason for the failure.
     */
    void onFailure(Throwable throwable) {
        setException(throwable);

        // TODO: should this be scheduled on another thread.
        executeCommand();
    }


    /**
     * This method is called when the command has failed to execute
     * successfully after maxRetries attempts.
     *
     * @param exception the reason for failed execution.
     */
    abstract void allAttemptsFailed(AerospikeException exception);

    public void execute() {
        //TODO: backoff based on in-flight command count.
        if (policy.totalTimeout > 0) {
            deadlineNanos =
                    System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(policy.totalTimeout);
        }

        executeCommand();
    }

    private void executeCommand() {
        // TODO: accommodate Policy.sleepBetweenRetries here.
        // Keep trying inline if executeOnce fails.
        boolean success = false;
        while (!success && shouldRetry()) {
            try {
                success = executeOnce();
            } catch (Throwable t) {
                setException(t);
            }
        }

        // All attempts have failed.
        // TODO: break function call cycle.
        if (!success) {
            allAttemptsFailed(exception);
        }
    }

    protected boolean isWrite() {
        return false;
    }

    protected void incrementCommandSentCounter() {
        commandSentCounter++;
    }

    private boolean executeOnce() {
        iteration++;

        // TODO: map stop conditions to the appropriate Exceptions.
        if (!shouldRetry()) {
            allAttemptsFailed(exception);
            return false;
        }

        sendRequest();
        return true;
    }

    private boolean shouldRetry() {
        return iteration <= (policy.maxRetries + 1) && !shouldHaltOnException() &&
                (System.nanoTime() < deadlineNanos);
    }

    private boolean shouldHaltOnException() {
        // TODO: the command should not be retried on some exceptions.
        return false;
    }

    private void setException(Throwable throwable) {
        // TODO: handle all exception cases.

        if (throwable instanceof AerospikeException) {
            AerospikeException ae = (AerospikeException) throwable;
            if (ae.getResultCode() == ResultCode.TIMEOUT) {
                exception = new AerospikeException.Timeout(policy, false);
                isClientTimeout = false;
            } else if (ae.getResultCode() == ResultCode.DEVICE_OVERLOAD) {
                exception = ae;
                isClientTimeout = false;
            } else {
                exception = ae;
            }
        } else {
            AerospikeException ae = new AerospikeException(throwable);
            ae.setIteration(iteration);
            ae.setPolicy(policy);
            ae.setInDoubt(isWrite(), commandSentCounter);
            exception = ae;
        }
    }
}
