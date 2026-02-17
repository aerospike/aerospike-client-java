/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.WritePolicy;

public final class TouchCommand extends SyncWriteCommand {
	private boolean failOnNotFound;
	private boolean touched;

	public TouchCommand(Cluster cluster, WritePolicy writePolicy, Key key, boolean failOnNotFound) {
		super(cluster, writePolicy, key);
		this.failOnNotFound = failOnNotFound;
	}

	@Override
	protected void writeBuffer() {
		setTouch(writePolicy, key);
	}

	@Override
	protected void parseResult(Node node, Connection conn) throws IOException {
		int resultCode = parseHeader(node, conn);

		if (resultCode == ResultCode.OK) {
			touched = true;
			return;
		}

		if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			if (failOnNotFound) {
				throw new AerospikeException(resultCode);
			}
			touched = false;
			return;
		}

		if (resultCode == ResultCode.FILTERED_OUT) {
			if (writePolicy.failOnFilteredOut) {
				throw new AerospikeException(resultCode);
			}
			touched = false;
			return;
		}

		throw new AerospikeException(resultCode);
	}

	public boolean getTouched() {
		return touched;
	}
}
