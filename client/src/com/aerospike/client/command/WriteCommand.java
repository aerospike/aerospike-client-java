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
import java.io.InputStream;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;

public final class WriteCommand extends SingleCommand {

	public WriteCommand(Cluster cluster, Key key) {
		super(cluster, key);
	}

	protected void parseResult(InputStream is) throws AerospikeException, IOException {
		// Read header.		
		readFully(is, receiveBuffer, MSG_TOTAL_HEADER_SIZE);
	
		long sz = Buffer.bytesToLong(receiveBuffer, 0);
		byte headerLength = receiveBuffer[8];
		int resultCode = receiveBuffer[13];
		int receiveSize = ((int) (sz & 0xFFFFFFFFFFFFL)) - headerLength;
				
		// Read remaining message bytes.
        if (receiveSize > 0) {
        	resizeReceiveBuffer(receiveSize);
    		readFully(is, receiveBuffer, receiveSize);
        }
        
	    if (resultCode != 0) {
	    	throw new AerospikeException(resultCode);        	
	    }        	
	}
}
