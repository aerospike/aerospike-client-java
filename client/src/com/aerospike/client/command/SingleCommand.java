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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.util.ThreadLocalData2;

public abstract class SingleCommand extends SyncCommand {
	private final Cluster cluster;
	private final Partition partition;

	public SingleCommand(Cluster cluster, Key key) {
		this.cluster = cluster;
		this.partition = new Partition(key);
		this.receiveBuffer = ThreadLocalData2.getBuffer();
	}
	
	public final void resizeReceiveBuffer(int size) {
		if (size > receiveBuffer.length) {
			receiveBuffer = ThreadLocalData2.resizeBuffer(size);
		}
	}
	
	protected final Node getNode() throws AerospikeException.InvalidNode { 
		return cluster.getNode(partition);
	}		
}
