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
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.proxy.client.KVSGrpc;
import com.aerospike.proxy.client.Kvs;

import io.grpc.MethodDescriptor;

public class ReadCommandProxy extends CommandProxy {
	private final RecordListener listener;
	final Key key;
	private final String[] binNames;
	private final boolean isOperation;

	public ReadCommandProxy(
		GrpcCallExecutor executor,
		RecordListener listener,
		Policy policy,
		Key key,
		String[] binNames
	) {
		super(KVSGrpc.getReadStreamingMethod(), executor, policy);
		this.listener = listener;
		this.key = key;
		this.binNames = binNames;
		this.isOperation = false;
	}

	public ReadCommandProxy(
		MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor,
		GrpcCallExecutor executor,
		RecordListener listener,
		Policy policy,
		Key key,
		boolean isOperation
	) {
		super(methodDescriptor, executor, policy);
		this.listener = listener;
		this.key = key;
		this.binNames = null;
		this.isOperation = isOperation;
	}

	@Override
	protected void writeCommand(Command command) {
		command.setRead(policy, key, binNames);
	}

	@Override
	protected void parseResult(Parser parser) {
		ProxyRecord proxyRecord = parseRecordResult(parser, isOperation,
			false, false);

		try {
			listener.onSuccess(key, proxyRecord.record);
		}
		catch (Throwable t) {
			logOnSuccessError(t);
		}
	}

	@Override
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}
}