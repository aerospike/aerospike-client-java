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
package com.aerospike.client.command;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Tran;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.policy.WritePolicy;
import java.io.IOException;

public final class TranWillRoll extends SyncWriteCommand {
	private final Tran tran;

	public TranWillRoll(Cluster cluster, Tran tran, WritePolicy writePolicy, Key key) {
		super(cluster, writePolicy, key);
		this.tran = tran;
	}

	@Override
	protected void writeBuffer() {
		setTranWillRoll(tran, key);
	}

	@Override
	protected void parseResult(Connection conn) throws IOException {
		int resultCode = parseHeader(conn);

		// BIN_EXISTS_ERROR is considered a success because it means a previous attempt already
		// succeeded in notifying the server that the MRT will be rolled forward.
		if (resultCode == ResultCode.OK || resultCode == ResultCode.BIN_EXISTS_ERROR) {
			return;
		}

		throw new AerospikeException(resultCode);
	}
}
