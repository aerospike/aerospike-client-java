/*
 * Copyright 2012-2018 Aerospike, Inc.
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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.luaj.vm2.LuaValue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.policy.QueryPolicy;

public final class QueryAggregateCommand extends MultiCommand {
	
	private final QueryPolicy policy;
	private final Statement statement;
	private final LuaInstance instance;
	private final BlockingQueue<LuaValue> inputQueue;

	public QueryAggregateCommand(
		QueryPolicy policy,
		Statement statement,
		LuaInstance instance,
		BlockingQueue<LuaValue> inputQueue,
		long clusterKey,
		boolean first
	) {
		super(statement.namespace, clusterKey, first);
		this.policy = policy;
		this.statement = statement;
		this.instance = instance;
		this.inputQueue = inputQueue;
	}

	@Override
	protected final void writeBuffer() throws AerospikeException {
		setQuery(policy, statement, false);
	}

	@Override
	protected void parseRow(Key key) throws IOException {		
		if (opCount != 1) {
			throw new AerospikeException("Query aggregate expected exactly one bin.  Received " + opCount);
		}

		// Parse aggregateValue.
		readBytes(8);	
		int opSize = Buffer.bytesToInt(dataBuffer, 0);
		byte particleType = dataBuffer[5];
		byte nameSize = dataBuffer[7];
		
		readBytes(nameSize);
		String name = Buffer.utf8ToString(dataBuffer, 0, nameSize);

		int particleBytesSize = (int) (opSize - (4 + nameSize));
		readBytes(particleBytesSize);
		
		if (! name.equals("SUCCESS")) {
			if (name.equals("FAILURE")) {
				Object value = Buffer.bytesToParticle(particleType, dataBuffer, 0, particleBytesSize);
				throw new AerospikeException(ResultCode.QUERY_GENERIC, value.toString());
			}
			else {
				throw new AerospikeException(ResultCode.PARSE_ERROR, "Query aggregate expected bin name SUCCESS.  Received " + name);
			}
		}
		
		LuaValue aggregateValue = instance.getLuaValue(particleType, dataBuffer, 0, particleBytesSize);
								
		if (! valid) {
			throw new AerospikeException.QueryTerminated();
		}

		if (aggregateValue != null) {
			try {
				inputQueue.put(aggregateValue);			
			}
			catch (InterruptedException ie) {
			}
		}
	}
}
