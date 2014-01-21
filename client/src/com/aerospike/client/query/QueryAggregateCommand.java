/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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
			LuaValue aggregateValue = instance.getValue(particleType, dataBuffer, 0, particleBytesSize);
						
			if (! name.equals("SUCCESS")) {
				throw new AerospikeException("Query aggregate expected bin name SUCCESS.  Received " + name);
			}
			
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
