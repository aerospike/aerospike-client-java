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
package com.aerospike.client.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.WritePolicy;

public final class ServerCommand extends MultiCommand {
	private final Statement statement;

	public ServerCommand(Cluster cluster, Node node, WritePolicy writePolicy, Statement statement) {
		super(cluster, writePolicy, node, true);
		this.statement = statement;
	}

	@Override
	protected boolean isWrite() {
		return true;
	}

	@Override
	protected final void writeBuffer() {
		setQuery(policy, statement, true, null);
	}

	@Override
	protected void parseRow(Key key) {
		// Server commands (Query/Execute UDF) should only send back a return code.
		// Keep parsing logic to empty socket buffer just in case server does
		// send records back.
		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 7;
			byte nameSize = dataBuffer[dataOffset++];
			int particleBytesSize = (int) (opSize - (4 + nameSize));
			dataOffset += nameSize + particleBytesSize;
	    }

		if (! valid) {
			throw new AerospikeException.QueryTerminated();
		}
	}
}
