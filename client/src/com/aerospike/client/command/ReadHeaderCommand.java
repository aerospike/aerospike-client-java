/*
 * Copyright 2012-2020 Aerospike, Inc.
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
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.Policy;

public class ReadHeaderCommand extends SyncCommand {
	private final Key key;
	private final Partition partition;
	private Record record;

	public ReadHeaderCommand(Cluster cluster, Policy policy, Key key) {
		super(cluster, policy);
		this.key = key;
		this.partition = Partition.read(cluster, policy, key);
	}

	@Override
	protected Node getNode() {
		return partition.getNodeRead(cluster);
	}

	@Override
	protected void writeBuffer() {
		setReadHeader(policy, key);
	}

	@Override
	protected void parseResult(Connection conn) throws IOException {
		// Read header.
		conn.readFully(dataBuffer, Command.MSG_TOTAL_HEADER_SIZE, Command.STATE_READ_HEADER);
		conn.updateLastUsed();

		int resultCode = dataBuffer[13] & 0xFF;

		if (resultCode == 0) {
			int generation = Buffer.bytesToInt(dataBuffer, 14);
			int expiration = Buffer.bytesToInt(dataBuffer, 18);
			record = new Record(null, generation, expiration);
			return;
		}

		if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			return;
		}

		if (resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(resultCode);
			}
			return;
		}

		throw new AerospikeException(resultCode);
	}

	@Override
	protected boolean prepareRetry(boolean timeout) {
		partition.prepareRetryRead(timeout);
		return true;
	}

	public Record getRecord() {
		return record;
	}
}
