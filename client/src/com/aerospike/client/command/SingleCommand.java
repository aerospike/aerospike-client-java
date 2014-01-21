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
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;

public abstract class SingleCommand extends SyncCommand {
	private final Cluster cluster;
	protected final Key key;
	private final Partition partition;

	public SingleCommand(Cluster cluster, Key key) {
		this.cluster = cluster;
		this.key = key;
		this.partition = new Partition(key);
	}
	
	protected final Node getNode() throws AerospikeException.InvalidNode { 
		return cluster.getNode(partition);
	}	

	protected final void emptySocket(Connection conn) throws IOException
	{
		// There should not be any more bytes.
		// Empty the socket to be safe.
		long sz = Buffer.bytesToLong(dataBuffer, 0);
		int headerLength = dataBuffer[8];
		int receiveSize = ((int)(sz & 0xFFFFFFFFFFFFL)) - headerLength;

		// Read remaining message bytes.
		if (receiveSize > 0)
		{
			sizeBuffer(receiveSize);
			conn.readFully(dataBuffer, receiveSize);
		}
	}
}
