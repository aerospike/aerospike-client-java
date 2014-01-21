/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.command;

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.policy.Policy;

public class ReadHeaderCommand extends SingleCommand {
	private final Policy policy;
	private Record record;

	public ReadHeaderCommand(Cluster cluster, Policy policy, Key key) {
		super(cluster, key);
		this.policy = (policy == null) ? new Policy() : policy;
	}
	
	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setReadHeader(key);
	}

	protected void parseResult(Connection conn) throws AerospikeException, IOException {
		// Read header.		
		conn.readFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);

		int resultCode = dataBuffer[13] & 0xFF;

        if (resultCode == 0) {
    		int generation = Buffer.bytesToInt(dataBuffer, 14);
    		int expiration = Buffer.bytesToInt(dataBuffer, 18);
        	record = new Record(null, null, generation, expiration);       	
        }
        else {
			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
				record = null;
			}
			else {
				throw new AerospikeException(resultCode);
			}
		}
		emptySocket(conn);
	}
	
	public Record getRecord() {
		return record;
	}
}
