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

import gnu.crypto.util.Base64;

import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.command.Buffer;

/**
 * Parse node partitions using new protocol. This is more code than a String.split() implementation, 
 * but it's faster because there are much fewer interim strings.
 */
public final class PartitionTokenizerNew {
	private static final String ReplicasName = "replicas-master";
	
	// Create reusable StringBuilder for performance.	
	protected final StringBuilder sb;
	protected final byte[] buffer;
	protected int length;
	protected int offset;
	
	public PartitionTokenizerNew(Connection conn) throws AerospikeException {
		// Use low-level info methods and parse byte array directly for maximum performance.
		// Send format:	   replicas-master\n
		// Receive format: replicas-master\t<ns1>:<base 64 encoded bitmap>;<ns2>:<base 64 encoded bitmap>... \n
		Info info = new Info(conn, ReplicasName);
		this.length = info.getLength();

		if (length == 0) {
			throw new AerospikeException.Parse(ReplicasName + " is empty");
		}
		this.buffer = info.getBuffer();
		this.offset = ReplicasName.length() + 1;  // Skip past name and tab
		this.sb = new StringBuilder(32);  // Max namespace length
	}
	
	public HashMap<String,Node[]> updatePartition(HashMap<String,Node[]> map, Node node) throws AerospikeException {
		int begin = offset;
		boolean copied = false;
		
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
				Node[] nodeArray = map.get(namespace);

				if (nodeArray == null) {
					if (! copied) {
						// Make shallow copy of map.
						map = new HashMap<String,Node[]>(map);
						copied = true;
					}
					nodeArray = new Node[Node.PARTITIONS];
					map.put(namespace, nodeArray);
				}

				int bitMapLength = offset - begin;		
				byte[] restoreBuffer = Base64.decode(buffer, begin, bitMapLength);

				for (int i = 0; i < Node.PARTITIONS; i++) {
					if ((restoreBuffer[i >> 3] & (0x80 >> (i & 7))) != 0) {
						//Log.info("Map: " + namespace + ',' + i + ',' + node);
						nodeArray[i] = node;
					}
				}
				begin = ++offset;
			}
			else {
				offset++;
			}
		}
		return (copied)? map : null;
	}
	
	private String getTruncatedResponse() {
		int max = (length > 200) ? 200 : length;
		return Buffer.utf8ToString(buffer, 0, max);		
	}
}
