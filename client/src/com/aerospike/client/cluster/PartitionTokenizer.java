/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.cluster;

import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;

/**
 * Parse node partitions. This is more code than a String.split() implementation, 
 * but it's faster because there are much fewer interim strings.
 */
public abstract class PartitionTokenizer {
	// Create reusable StringBuilder for performance.	
	protected final StringBuilder sb;
	protected final byte[] buffer;
	protected int length;
	protected int offset;
	
	public PartitionTokenizer(Connection conn, String name) throws AerospikeException {
		// Use low-level info methods and parse byte array directly for maximum performance.
		// Send format:    replicas-write\n
		// Receive format: replicas-write\t<ns1>:<partition id1>;<ns2>:<partition id2>...\n
		Info info = new Info(conn, name);
		this.length = info.getLength();

		if (length == 0) {
			throw new AerospikeException.Parse("replicas-write is empty");
		}
		this.buffer = info.getBuffer();
		this.offset = name.length() + 1;  // Skip past name and tab
		this.sb = new StringBuilder(32);  // Max namespace length
	}
	
	//public abstract Partition getNext() throws AerospikeException;
	public abstract void updatePartition(HashMap<String,Node[]> partitionWriteMap, Node node) throws AerospikeException;
}
