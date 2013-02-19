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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.command.Buffer;

/**
 * Parse node partitions. This is more code than a String.split() implementation, 
 * but it's faster because there are much fewer interim strings.
 */
public final class PartitionTokenizer {
	// Create reusable StringBuilder for performance.	
	private final StringBuilder sb;
	private final byte[] buffer;
	private int length;
	private int offset;
	
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
	
	public Partition getNext() throws AerospikeException {
		int begin = offset;
		
		while (offset < length) {
			if (buffer[offset] == ':') {
				// Parse namespace.
				String namespace = Buffer.utf8ToString(buffer, begin, offset - begin, sb).trim();
				
				if (namespace.length() <= 0 || namespace.length() >= 32) {
					String response = getTruncatedResponse();
					throw new AerospikeException.Parse("Invalid partition namespace " +
						namespace + ". Response=" + response);
				}
				begin = ++offset;
				
				// Parse partition id.
				while (offset < length) {
					byte b = buffer[offset];
					
					if (b == ';' || b == '\n') {
						break;
					}
					offset++;
				}
				
				if (offset == begin) {
					String response = getTruncatedResponse();
					throw new AerospikeException.Parse("Empty partition id for namespace " +
						namespace + ". Response=" + response);										
				}
				
				int partitionId = Buffer.utf8DigitsToInt(buffer, begin, offset);
								
				if (partitionId < 0 || partitionId >= Node.PARTITIONS) {
					String response = getTruncatedResponse();
					String partitionString = Buffer.utf8ToString(buffer, begin, offset - begin);
					throw new AerospikeException.Parse("Invalid partition id " + partitionString +
						" for namespace " + namespace + ". Response=" + response);					
				}
				begin = ++offset;
				return new Partition(namespace, partitionId);
			}
			offset++;
		}
		return null;
	}
	
	private String getTruncatedResponse() {
		int max = (length > 200) ? 200 : length;
		return Buffer.utf8ToString(buffer, 0, max);		
	}
}
