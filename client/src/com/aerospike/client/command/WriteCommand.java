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
