/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public final class WriteCommand extends SingleCommand {
	private final WritePolicy policy;
	private final Bin[] bins;
	private final Operation.Type operation;

	public WriteCommand(Cluster cluster, WritePolicy policy, Key key, Bin[] bins, Operation.Type operation) {
		super(cluster, key);
		this.policy = (policy == null) ? new WritePolicy() : policy;
		this.bins = bins;
		this.operation = operation;
	}

	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setWrite(policy, operation, key, bins);
	}

	protected void parseResult(Connection conn) throws AerospikeException, IOException {
		// Read header.		
		conn.readFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);
	
		int resultCode = dataBuffer[13] & 0xFF;
		
	    if (resultCode != 0) {
	    	throw new AerospikeException(resultCode);        	
	    }        	
	    emptySocket(conn);
	}
}
