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

import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;

public final class ExecuteCommandProxy extends ReadCommandProxy {
    private final ExecuteListener executeListener;
    private final WritePolicy writePolicy;
    private final Key key;
 	private final String packageName;
	private final String functionName;
	private final Value[] args;

    public ExecuteCommandProxy(
    	GrpcCallExecutor grpcCallExecutor,
    	ExecuteListener executeListener,
    	WritePolicy writePolicy,
    	Key key,
    	String packageName,
    	String functionName,
    	Value[] args
    ) {
    	super(grpcCallExecutor, null, writePolicy, key, false);
        this.executeListener = executeListener;
        this.writePolicy = writePolicy;
        this.key = key;
        this.packageName = packageName;
        this.functionName = functionName;
        this.args = args;
    }

	@Override
	void writePayload() {
        serde.setUdf(writePolicy, key, packageName, functionName, args);
	}

	@Override
	protected void parseResult(Parser parser) {
		Record record = parseRecordResult(parser);
		Object obj = parseEndResult(record);
		executeListener.onSuccess(key, obj);
	}

	private static Object parseEndResult(Record record) {
		if (record == null || record.bins == null) {
			return null;
		}

		Map<String,Object> map = record.bins;

		Object obj = map.get("SUCCESS");

		if (obj != null) {
			return obj;
		}

		// User defined functions don't have to return a value.
		if (map.containsKey("SUCCESS")) {
			return null;
		}

		obj = map.get("FAILURE");

		if (obj != null) {
			throw new AerospikeException(obj.toString());
		}
		throw new AerospikeException("Invalid UDF return value");
	}

	@Override
	protected void handleNotFound(int resultCode) {
		throw new AerospikeException(resultCode);
	}

	@Override
    void allAttemptsFailed(AerospikeException exception) {
		executeListener.onFailure(exception);
    }
}
