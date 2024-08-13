/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.async;

import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.RecordParser;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.policy.WritePolicy;

public final class AsyncExecute extends AsyncWriteBase {
	private final ExecuteListener executeListener;
	private final String packageName;
	private final String functionName;
	private final Value[] args;
	private Record record;

	public AsyncExecute(
		Cluster cluster,
		ExecuteListener listener,
		WritePolicy writePolicy,
		Key key,
		String packageName,
		String functionName,
		Value[] args
	) {
		super(cluster, writePolicy, key);
		this.executeListener = listener;
		this.packageName = packageName;
		this.functionName = functionName;
		this.args = args;
	}

	@Override
	protected void writeBuffer() {
		setUdf(writePolicy, key, packageName, functionName, args);
	}

	@Override
	protected boolean parseResult() {
		RecordParser rp = new RecordParser(dataBuffer, dataOffset, receiveSize);
		rp.parseFields(policy.txn, key, true);

		if (rp.resultCode == ResultCode.OK) {
			record = rp.parseRecord(false);
			return true;
		}

		if (rp.resultCode == ResultCode.UDF_BAD_RESPONSE) {
			record = rp.parseRecord(false);
			handleUdfError(rp.resultCode);
			return true;
		}

		if (rp.resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(rp.resultCode);
			}
			return true;
		}

		throw new AerospikeException(rp.resultCode);
	}

	private void handleUdfError(int resultCode) {
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
		catch (Throwable e) {
			// Use generic exception if parse error occurs.
			throw new AerospikeException(resultCode, ret);
		}

		throw new AerospikeException(code, message);
	}

	@Override
	protected void onSuccess() {
		if (executeListener != null) {
			Object obj = parseEndResult();
			executeListener.onSuccess(key, obj);
		}
	}

	@Override
	protected void onFailure(AerospikeException e) {
		if (executeListener != null) {
			executeListener.onFailure(e);
		}
	}

	private Object parseEndResult() {
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
}
