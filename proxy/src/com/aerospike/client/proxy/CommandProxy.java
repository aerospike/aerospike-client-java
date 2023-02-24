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
	protected final GrpcCallExecutor executor;
	final Policy policy;
	private final int iteration = 1;

	public CommandProxy(GrpcCallExecutor executor, Policy policy) {
		this.executor = executor;
		this.policy = policy;
	}

	final void execute() {
		// TODO: Only set deadline if plan to retry?
		/*
		if (policy.totalTimeout > 0) {
			deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(policy.totalTimeout);
		}
		*/
		executeCommand();
	}

	private void executeCommand() {
		// TODO: Only need iteration counter if plan to retry?
		// iteration++;

		Command command = new Command(policy.socketTimeout, policy.totalTimeout, policy.maxRetries);
		writeCommand(command);

		ByteString payload = ByteString.copyFrom(command.dataBuffer, 0, command.dataOffset);

		executor.execute(new GrpcStreamingCall(getGrpcMethod(), payload, policy, iteration,
				isUnaryCall(),
				new StreamObserver<Kvs.AerospikeResponsePayload>() {
					@Override
					public void onNext(Kvs.AerospikeResponsePayload value) {
						try {
							onResponse(value);
						}
						catch (Throwable t) {
							onFailure(t, value.getInDoubt());
						}
					}

					@Override
					public void onError(Throwable t) {
						// TODO: What kind of errors returned here. If timeouts, should inDoubt be true?
						onFailure(t, false);
					}

					@Override
					public void onCompleted() {
					}
				}));
	}

	protected MethodDescriptor<Kvs.AerospikeRequestPayload,
			Kvs.AerospikeResponsePayload> getGrpcMethod() {
		// TODO should be implemented by all subclasses and this method
		//  should be made abstract.
		throw new RuntimeException("not implemented");
	}

	protected boolean isUnaryCall() {
		return true;
	}

	void onResponse(Kvs.AerospikeResponsePayload response) {
		byte[] bytes = response.getPayload().toByteArray();
		Parser parser = new Parser(bytes, response.getStatus());
		parser.parseProto();
		parseResult(parser, response.getInDoubt());
	}

	private void onFailure(Throwable t, boolean inDoubt) {
		// TODO: Retry on connection errors?
		if (t instanceof AerospikeException) {
			AerospikeException ae = (AerospikeException)t;

			if (ae.getResultCode() == ResultCode.TIMEOUT) {
				ae = new AerospikeException.Timeout(policy, false);
			}
			notifyFailure(ae, inDoubt);
			return;
		}

		if (t instanceof StatusRuntimeException) {
			AerospikeException ae = toAerospikeException((StatusRuntimeException)t);
			notifyFailure(ae, inDoubt);
			return;
		}

		AerospikeException ae = new AerospikeException(ResultCode.CLIENT_ERROR, t);
		notifyFailure(ae, inDoubt);
		return;
	}

	private AerospikeException toAerospikeException(StatusRuntimeException sre) {
		Status.Code code = sre.getStatus().getCode();

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
				return new AerospikeException(ResultCode.CLIENT_ERROR, "gRPC status code=" + code, sre);

			case INVALID_ARGUMENT:
				return new AerospikeException(ResultCode.SERIALIZE_ERROR, sre);

			case DEADLINE_EXCEEDED:
				return new AerospikeException.Timeout(policy, iteration);

			case PERMISSION_DENIED:
				return new AerospikeException(ResultCode.FAIL_FORBIDDEN, sre);

			case RESOURCE_EXHAUSTED:
				return new AerospikeException(ResultCode.QUOTA_EXCEEDED, sre);

			case UNAVAILABLE:
				return new AerospikeException.Backoff(ResultCode.MAX_ERROR_RATE);

			case UNAUTHENTICATED:
				return new AerospikeException(ResultCode.NOT_AUTHENTICATED);

			default:
				return new AerospikeException("gRPC code " + code, sre);
		}
	}

	// TODO: Under what error codes should we retry?
	/*
	private void retry() {
		executor.getEventLoop().schedule(this::executeCommand, policy.sleepBetweenRetries, TimeUnit.MILLISECONDS);
	}
	*/

	private void notifyFailure(AerospikeException ae, boolean inDoubt) {
		ae.setPolicy(policy);
		ae.setIteration(iteration);
		ae.setInDoubt(inDoubt);

		try {
			onFailure(ae);
		}
		catch (Throwable t) {
			Log.error("onFailure() error: " + Util.getStackTrace(t));
		}
	}

	static void logOnSuccessError(Throwable t) {
		Log.error("onSuccess() error: " + Util.getStackTrace(t));
	}

	protected void onSuccess() {
		// Overridden by multi commands.
	}

	abstract void writeCommand(Command command);
	abstract void parseResult(Parser parser, Boolean inDoubt);
	abstract void onFailure(AerospikeException ae);
}
