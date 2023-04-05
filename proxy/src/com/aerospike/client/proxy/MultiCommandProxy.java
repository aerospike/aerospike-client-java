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
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.query.BVal;
import com.aerospike.proxy.client.Kvs;

import io.grpc.MethodDescriptor;

public abstract class MultiCommandProxy extends CommandProxy {
	boolean hasNext;

	public MultiCommandProxy(
		MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor,
		GrpcCallExecutor executor,
		Policy policy
	) {
		super(methodDescriptor, executor, policy);
	}

	@Override
	void onResponse(Kvs.AerospikeResponsePayload response) {
		// Check response status for client errors (negative error codes).
		// Server errors are checked in response payload in Parser.
		int status = response.getStatus();
		this.hasNext = response.getHasNext();

		if (status != 0) {
			notifyFailure(new AerospikeException(status));
			return;
		}

		byte[] bytes = response.getPayload().toByteArray();
		Parser parser = new Parser(bytes);
		parser.parseProto();
		parseResult(parser);


		// TODO: Integrate the following code just like batch.
/*
		// Check final response status for client errors (negative error codes).
		resultCode = response.getStatus();
		hasNext = response.getHasNext();

		if (resultCode != 0 && !hasNext) {
			notifyFailure(new AerospikeException(resultCode));
			return;
		}

		// Server errors are checked in response payload in Parser.
		byte[] bytes = response.getPayload().toByteArray();
		Parser parser = new Parser(bytes);
		parser.parseProto();
		int rc = parser.parseHeader();

		if (hasNext) {
			if (resultCode == 0) {
				resultCode = rc;
			}
			parser.skipKey();
			parseResult(parser);
			return;
		}

		if (rc == ResultCode.OK) {
			try {
				onSuccess();
			}
			catch (Throwable t) {
				logOnSuccessError(t);
			}
		}
		else {
			notifyFailure(new AerospikeException(rc));
		}
*/
	}

	final RecordProxy parseRecordResult(
		Parser parser,
		boolean isOperation,
		boolean parseKey,
		boolean parseBVal
	) {
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

		return new RecordProxy(resultCode, key, record, bVal);
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

	abstract void parseResult(Parser parser);
}
