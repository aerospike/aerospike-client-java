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
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcStreamingCall;
import com.aerospike.client.util.Util;
import com.aerospike.proxy.client.Kvs;
import com.google.protobuf.ByteString;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public abstract class CommandProxy {
	final Policy policy;
	private final GrpcCallExecutor executor;
	private final MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor;
	private long deadlineNanos;
	private int sendTimeoutMillis;
	private int iteration = 1;
	private final boolean isSingle;
	boolean inDoubt;

	public CommandProxy(
		MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor,
		GrpcCallExecutor executor,
		Policy policy,
		boolean isSingle
	) {
		this.methodDescriptor = methodDescriptor;
		this.executor = executor;
		this.policy = policy;
		this.isSingle = isSingle;
	}

	final void execute() {
		if (policy.totalTimeout > 0) {
			deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(policy.totalTimeout);
			sendTimeoutMillis = (policy.socketTimeout > 0 && policy.socketTimeout < policy.totalTimeout)?
								 policy.socketTimeout : policy.totalTimeout;
		}
		else {
			deadlineNanos = 0; // No total deadline.
			sendTimeoutMillis = (policy.socketTimeout > 0)? policy.socketTimeout : 10_000;
		}

		executeCommand();
	}

	private void executeCommand() {
		long sendDeadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sendTimeoutMillis);

		Kvs.AerospikeRequestPayload.Builder builder = getRequestBuilder();

		executor.execute(new GrpcStreamingCall(methodDescriptor, builder,
			policy, iteration, deadlineNanos, sendDeadlineNanos, isSingle,
			new StreamObserver<Kvs.AerospikeResponsePayload>() {
				@Override
				public void onNext(Kvs.AerospikeResponsePayload response) {
					try {
						inDoubt |= response.getInDoubt();
						onResponse(response);
					}
					catch (Throwable t) {
						onFailure(t);
						// Re-throw to abort at the proxy/
						throw t;
					}
				}

				@Override
				public void onError(Throwable t) {
					inDoubt = true;
					onFailure(t);
				}

				@Override
				public void onCompleted() {
				}
			}));
	}

	boolean retry() {
		if (iteration > policy.maxRetries) {
			return false;
		}

		if (policy.totalTimeout > 0) {
			long remaining = deadlineNanos - System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(policy.sleepBetweenRetries);

			if (remaining <= 0) {
				return false;
			}
		}

		iteration++;
		executor.getEventLoop().schedule(this::retryNow, policy.sleepBetweenRetries, TimeUnit.MILLISECONDS);
		return true;
	}

	private void retryNow() {
		try {
			executeCommand();
		}
		catch (AerospikeException ae) {
			notifyFailure(ae);
		}
		catch (Throwable t) {
			notifyFailure(new AerospikeException(ResultCode.CLIENT_ERROR, t));
		}
	}

	private void onFailure(Throwable t) {
		AerospikeException ae;

		try {
			if (t instanceof AerospikeException) {
				ae = (AerospikeException)t;

				if (ae.getResultCode() == ResultCode.TIMEOUT) {
					ae = new AerospikeException.Timeout(policy, false);
				}
			}
			else if (t instanceof StatusRuntimeException) {
				StatusRuntimeException sre = (StatusRuntimeException)t;
				Status.Code code = sre.getStatus().getCode();

				if (code == Status.Code.UNAVAILABLE) {
					if (retry()) {
						return;
					}
				}
				ae = toAerospikeException(sre, code);
			}
			else {
				ae = new AerospikeException(ResultCode.CLIENT_ERROR, t);
			}
		}
		catch (AerospikeException ae2) {
			ae = ae2;
		}
		catch (Throwable t2) {
			ae = new AerospikeException(ResultCode.CLIENT_ERROR, t2);
		}

		notifyFailure(ae);
	}

	private AerospikeException toAerospikeException(StatusRuntimeException sre, Status.Code code) {
		switch (code) {
			case CANCELLED:
			case UNKNOWN:
			case NOT_FOUND:
			case ALREADY_EXISTS:
			case FAILED_PRECONDITION:
			case OUT_OF_RANGE:
			case UNIMPLEMENTED:
			case INTERNAL:
				return new AerospikeException(ResultCode.CLIENT_ERROR, "gRPC status code=" + code, sre);

			case ABORTED:
			case DATA_LOSS:
				return new AerospikeException(ResultCode.SERVER_ERROR, "gRPC status " +
					"code=" + code, sre);

			case INVALID_ARGUMENT:
				return new AerospikeException(ResultCode.SERIALIZE_ERROR, sre);

			case DEADLINE_EXCEEDED:
				return new AerospikeException.Timeout(policy, iteration);

			case PERMISSION_DENIED:
				return new AerospikeException(ResultCode.FAIL_FORBIDDEN, sre);

			case RESOURCE_EXHAUSTED:
				return new AerospikeException(ResultCode.QUOTA_EXCEEDED, sre);

			case UNAUTHENTICATED:
				return new AerospikeException(ResultCode.NOT_AUTHENTICATED, sre);

			case UNAVAILABLE:
				return new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, sre);

			default:
				return new AerospikeException("gRPC code " + code, sre);
		}
	}

	final void notifyFailure(AerospikeException ae) {
		try {
			ae.setPolicy(policy);
			ae.setIteration(iteration);
			ae.setInDoubt(inDoubt);
			onFailure(ae);
		}
		catch (Throwable t) {
			Log.error("onFailure() error: " + Util.getStackTrace(t));
		}
	}

	static void logOnSuccessError(Throwable t) {
		Log.error("onSuccess() error: " + Util.getStackTrace(t));
	}

	Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
		Command command = new Command(policy.socketTimeout, policy.totalTimeout, policy.maxRetries);
		writeCommand(command);

		ByteString payload = ByteString.copyFrom(command.dataBuffer, 0, command.dataOffset);
		return Kvs.AerospikeRequestPayload.newBuilder().setPayload(payload);
	}

	abstract void writeCommand(Command command);
	abstract void onResponse(Kvs.AerospikeResponsePayload response);
	abstract void onFailure(AerospikeException ae);
}
