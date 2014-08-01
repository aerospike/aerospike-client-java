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
package com.aerospike.client.query;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.luaj.vm2.LuaValue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.policy.Policy;

public final class QueryAggregateCommand extends QueryCommand {
	
	private final LuaInstance instance;
	private final BlockingQueue<LuaValue> inputQueue;

	public QueryAggregateCommand(Node node, Policy policy, Statement statement, LuaInstance instance, BlockingQueue<LuaValue> inputQueue) {
		super(node, policy, statement);
		this.instance = instance;
		this.inputQueue = inputQueue;
	}
	
	@Override
	protected boolean parseRecordResults(int receiveSize) 
		throws AerospikeException, IOException {
		// Read/parse remaining message bytes one record at a time.
		dataOffset = 0;
		
		while (dataOffset < receiveSize) {
    		readBytes(MSG_REMAINING_HEADER_SIZE);    		
			int resultCode = dataBuffer[5] & 0xFF;
			
			if (resultCode != 0) {
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
					return false;
				}
				throw new AerospikeException(resultCode);
			}

			byte info3 = dataBuffer[3];
			
			// If this is the end marker of the response, do not proceed further
			if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST) {
				return false;
			}
			
			int fieldCount = Buffer.bytesToShort(dataBuffer, 18);
			int opCount = Buffer.bytesToShort(dataBuffer, 20);
			
			parseKey(fieldCount);
			
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
					throw new AerospikeException(ResultCode.QUERY_GENERIC, "Query aggregate expected bin name SUCCESS.  Received " + name);
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
		return true;
	}
}
