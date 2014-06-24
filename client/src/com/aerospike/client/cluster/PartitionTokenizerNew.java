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

import gnu.crypto.util.Base64;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
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
	
	public HashMap<String,AtomicReferenceArray<Node>> updatePartition(HashMap<String,AtomicReferenceArray<Node>> map, Node node) throws AerospikeException {
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
				AtomicReferenceArray<Node> nodeArray = map.get(namespace);

				if (nodeArray == null) {
					if (! copied) {
						// Make shallow copy of map.
						map = new HashMap<String,AtomicReferenceArray<Node>>(map);
						copied = true;
					}
					nodeArray = new AtomicReferenceArray<Node>(new Node[Node.PARTITIONS]);
					map.put(namespace, nodeArray);
				}

				int bitMapLength = offset - begin;		
				byte[] restoreBuffer = Base64.decode(buffer, begin, bitMapLength);

				for (int i = 0; i < Node.PARTITIONS; i++) {
					if ((restoreBuffer[i >> 3] & (0x80 >> (i & 7))) != 0) {
						//Log.info("Map: " + namespace + ',' + i + ',' + node);
						
						// Use lazy set because there is only one producer thread. In addition,
						// there is a one second delay due to the cluster tend polling interval.  
						// An extra millisecond for a node change will not make a difference and 
						// overall performance is improved.
						nodeArray.lazySet(i, node);
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
