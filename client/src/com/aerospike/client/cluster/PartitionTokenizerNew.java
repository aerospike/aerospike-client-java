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
import com.aerospike.client.command.Buffer;

/**
 * Parse node partitions. This is more code than a String.split() implementation, 
 * but it's faster because there are much fewer interim strings.
 */
public final class PartitionTokenizerNew extends PartitionTokenizer {
	// Create reusable StringBuilder for performance.	
	
	public PartitionTokenizerNew(Connection conn, String name) throws AerospikeException {
		super(conn, name);
	}
	
	/*public Partition getNext() throws AerospikeException {
		int begin = offset;		
		while (offset < length) {
			if (buffer[offset] == ':') {
				// Parse namespace.
				String namespace = Buffer.utf8ToString(buffer, begin, offset - begin, sb).trim();
				//System .out.println("Namespace: " + namespace);
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
	}*/
	public HashMap<String,Node[]> updatePartition(HashMap<String,Node[]> partitionWriteMap, Node node) throws AerospikeException {
		HashMap<String,Node[]> map = partitionWriteMap;
		int begin = offset;
		Node[] nodeArray;
		
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
				int bitMapLength = offset - begin;
				byte[] restoreBuffer = new byte[(bitMapLength*3)/4];
				
				restoreBuffer = Base64.decode(buffer, begin, offset);
				nodeArray = map.get(namespace);
				boolean copied = false;
				if (nodeArray == null) {
							if (! copied) {
								// Make shallow copy of map.
								map = new HashMap<String,Node[]>(map);
								copied = true;
							}
							nodeArray = new Node[Node.PARTITIONS];
							map.put(namespace, nodeArray);
						}

				for (int i = 0; i < Node.PARTITIONS; i++) {
					if ((restoreBuffer[i >> 3] & (0x80 >> (i & 7))) != 0) {
						nodeArray[i] = node;
						if (copied) {
							partitionWriteMap = map;
						}
					}
				}
				begin = ++offset;
			}
			offset++;
		}
		return map;
	}
	
	private String getTruncatedResponse() {
		int max = (length > 200) ? 200 : length;
		return Buffer.utf8ToString(buffer, 0, max);		
	}
}
