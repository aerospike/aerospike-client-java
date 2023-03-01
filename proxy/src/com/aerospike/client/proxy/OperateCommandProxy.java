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
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.proxy.client.KVSGrpc;

public final class OperateCommandProxy extends ReadCommandProxy {
	private final OperateArgs args;

	public OperateCommandProxy(
		GrpcCallExecutor executor,
		RecordListener listener,
		Policy policy,
		Key key,
		OperateArgs args
	) {
		super(KVSGrpc.getOperateStreamingMethod(), executor, listener, policy, key, true);
		this.args = args;
	}

	@Override
	void writeCommand(Command command) {
		command.setOperate(args.writePolicy, key, args);
	}

	@Override
	protected void handleNotFound(int resultCode) {
		// Only throw not found exception for command with write operations.
		// Read-only command operations return a null record.
		if (args.hasWrite) {
			throw new AerospikeException(resultCode);
		}
	}
}
