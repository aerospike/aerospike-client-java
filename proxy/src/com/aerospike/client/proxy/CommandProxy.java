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
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcStreamingCall;
import com.aerospike.client.query.BVal;
import com.aerospike.client.util.Util;
import com.aerospike.proxy.client.Kvs;
import com.google.protobuf.ByteString;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public abstract class CommandProxy {
	protected final Policy policy;
	private final GrpcCallExecutor executor;
	private final MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor;
	private long deadline;
	private int iteration = 1;
	boolean inDoubt;

	public CommandProxy(
		MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor,
		GrpcCallExecutor executor,
		Policy policy
	) {
		this.methodDescriptor = methodDescriptor;
		this.executor = executor;
		this.policy = policy;
	}

	final void execute() {
		long ms;

		if (policy.totalTimeout > 0) {
			ms = policy.totalTimeout;
		}
		else if (policy.socketTimeout > 0) {
			ms = policy.socketTimeout;
		}
		else {
			ms = 30000;
		}

		deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms);
		executeCommand();
	}

	private void executeCommand() {
		Kvs.AerospikeRequestPayload.Builder builder = getRequestBuilder();
		executor.execute(new GrpcStreamingCall(methodDescriptor,
			builder, policy, iteration, deadline,
			new StreamObserver<Kvs.AerospikeResponsePayload>() {
				@Override
				public void onNext(Kvs.AerospikeResponsePayload response) {
					try {
						// Check response status for client errors (negative error codes).
						// Server errors are checked in response payload in Parser.
						int status = response.getStatus();

						if (status != 0) {
							setInDoubt(response.getInDoubt());
							notifyFailure(new AerospikeException(status));
							return;
						}
						onResponse(response);
					}
					catch (Throwable t) {
						onFailure(t, response.getInDoubt());
					}
				}

				@Override
				public void onError(Throwable t) {
					// TODO: What kind of errors returned here. If timeouts, should inDoubt always be true?
					onFailure(t, inDoubt);
				}

				@Override
				public void onCompleted() {
				}
			}));
	}

	void onResponse(Kvs.AerospikeResponsePayload response) {
		byte[] bytes = response.getPayload().toByteArray();
		Parser parser = new Parser(bytes);
		parser.parseProto();
		parseResult(parser, !response.getHasNext());
	}

	private boolean retry() {
		if (iteration > policy.maxRetries) {
			return false;
		}

		if (policy.totalTimeout > 0) {
			long remaining = deadline - System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(policy.sleepBetweenRetries);

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

	private void onFailure(Throwable t, boolean doubt) {
		setInDoubt(doubt);

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

			case UNAUTHENTICATED:
				return new AerospikeException(ResultCode.NOT_AUTHENTICATED, sre);

			case UNAVAILABLE:
				return new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, sre);

			default:
				return new AerospikeException("gRPC code " + code, sre);
		}
	}

	final void setInDoubt(boolean doubt) {
		this.inDoubt |= doubt;
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

	protected static void logOnSuccessError(Throwable t) {
		Log.error("onSuccess() error: " + Util.getStackTrace(t));
	}

	protected Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
		Command command = new Command(policy.socketTimeout, policy.totalTimeout, policy.maxRetries);
		writeCommand(command);

		ByteString payload = ByteString.copyFrom(command.dataBuffer, 0, command.dataOffset);
		return Kvs.AerospikeRequestPayload.newBuilder().setPayload(payload);
	}

	/**
	 * Parse the Aerospike record byes accumulated in the parser.
	 *
	 * @param parser      the parser
	 * @param isOperation indicates if the payload is the result of an
	 *                    operation.
	 * @param parseKey    indicates if key is to be parsed
	 * @param parseBVal   indicates if bVal should be parsed
	 * @return proxy record
	 */
	protected final ProxyRecord parseRecordResult(Parser parser,
												  boolean isOperation, boolean parseKey,
												  boolean parseBVal) {
		Record record = null;
		Key key = null;
		BVal bVal = parseBVal ? new BVal() : null;
		int resultCode = parser.parseHeader();

		switch (resultCode) {
			case ResultCode.OK:
				if (parser.opCount == 0) {
					// Bin data was not returned.
					record = new Record(null, parser.generation, parser.expiration);
				}
				else {
					if (parseKey) {
						key = parser.parseKey(bVal);
					}
					else {
						parser.skipKey();
					}
					record = parser.parseRecord(isOperation);
				}
				break;

			case ResultCode.KEY_NOT_FOUND_ERROR:
				handleNotFound(resultCode);
				break;

			case ResultCode.FILTERED_OUT:
				if (policy.failOnFilteredOut) {
					throw new AerospikeException(resultCode);
				}
				break;

			case ResultCode.UDF_BAD_RESPONSE:
				parser.skipKey();
				record = parser.parseRecord(isOperation);
				handleUdfError(record, resultCode);
				break;

			default:
				throw new AerospikeException(resultCode);
		}

		return new ProxyRecord(resultCode, key, record, bVal);
	}

	protected void handleNotFound(int resultCode) {
		// Do nothing in default case. Record will be null.
	}

	protected void handleUdfError(Record record, int resultCode) {
		String ret = (String)record.bins.get("FAILURE");

		if (ret == null) {
			throw new AerospikeException(resultCode);
		}

		String message;
		int code;

		try {
			String[] list = ret.split(":");
			code = Integer.parseInt(list[2].trim());
			message = list[0] + ':' + list[1] + ' ' + list[3];
		}
		catch (Exception e) {
			// Use generic exception if parse error occurs.
			throw new AerospikeException(resultCode, ret);
		}

		throw new AerospikeException(code, message);
	}

	protected abstract void writeCommand(Command command);

	protected abstract void parseResult(Parser parser, boolean isLast);

	protected abstract void onFailure(AerospikeException ae);
}
