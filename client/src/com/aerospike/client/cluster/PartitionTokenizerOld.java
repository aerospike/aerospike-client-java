/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.cluster;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.command.Buffer;

/**
 * Parse node partitions using old protocol. This is more code than a String.split() implementation, 
 * but it's faster because there are much fewer interim strings.
 */
public final class PartitionTokenizerOld {	
	private static final String ReplicasName = "replicas-write";
	
	// Create reusable StringBuilder for performance.	
	protected final StringBuilder sb;
	protected final byte[] buffer;
	protected int length;
	protected int offset;
	
	public PartitionTokenizerOld(Connection conn) throws AerospikeException {
		// Use low-level info methods and parse byte array directly for maximum performance.
		// Send format:    replicas-write\n
		// Receive format: replicas-write\t<ns1>:<partition id1>;<ns2>:<partition id2>...\n
		Info info = new Info(conn, ReplicasName);
		this.length = info.getLength();

		if (length == 0) {
			throw new AerospikeException.Parse(ReplicasName + " is empty");
		}
		this.buffer = info.getBuffer();
		this.offset = ReplicasName.length() + 1;  // Skip past name and tab
		this.sb = new StringBuilder(32);  // Max namespace length
	}
	
	public HashMap<String,AtomicReferenceArray<Node>> updatePartition(HashMap<String,AtomicReferenceArray<Node>> map, Node node) throws AerospikeException {		
		Partition partition;
		boolean copied = false;
		
		while ((partition = getNext()) != null) {
			AtomicReferenceArray<Node> nodeArray = map.get(partition.namespace);
			
			if (nodeArray == null) {
				if (! copied) {
					// Make shallow copy of map.
					map = new HashMap<String,AtomicReferenceArray<Node>>(map);
					copied = true;
				}
				nodeArray = new AtomicReferenceArray<Node>(new Node[Node.PARTITIONS]);
				map.put(partition.namespace, nodeArray);
			}
			// Log.debug(partition.toString() + ',' + node.getName());
			nodeArray.lazySet(partition.partitionId, node);
		}
		return (copied)? map : null;
	}
	
	private Partition getNext() throws AerospikeException {
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
