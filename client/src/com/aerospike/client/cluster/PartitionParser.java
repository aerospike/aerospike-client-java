/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
import com.aerospike.client.Log;
import com.aerospike.client.command.Buffer;

/**
 * Parse node's master (and optionally prole) partitions.
 */
public final class PartitionParser {
	static final String PartitionGeneration = "partition-generation";
	static final String ReplicasMaster = "replicas-master";
	static final String ReplicasAll = "replicas-all";

	private HashMap<String,AtomicReferenceArray<Node>[]> map;
	private final StringBuilder sb;
	private final byte[] buffer;
	private final int partitionCount;
	private int length;
	private int offset;
	private int generation;
	private boolean copied;
	
	public PartitionParser(Connection conn, Node node, HashMap<String,AtomicReferenceArray<Node>[]> map, int partitionCount, boolean requestProleReplicas) {
		// Send format 1:  partition-generation\nreplicas-master\n
		// Send format 2:  partition-generation\nreplicas-all\n
		this.partitionCount = partitionCount;
		this.map = map;
		
		String command = (requestProleReplicas)? ReplicasAll : ReplicasMaster;
		Info info = new Info(conn, PartitionGeneration, command);
		this.length = info.getLength();

		if (length == 0) {
			throw new AerospikeException.Parse("Partition info is empty");
		}
		this.buffer = info.getBuffer();

		// Create reusable StringBuilder for performance.
		this.sb = new StringBuilder(32);  // Max namespace length
		
		generation = parseGeneration();
		
		if (requestProleReplicas) {
			parseReplicasAll(node);
		}
		else {
			parseReplicasMaster(node);		
		}
	}
	
	public int getGeneration() {
		return generation;
	}
	
	public boolean isPartitionMapCopied() {
		return copied;
	}
	
	public HashMap<String,AtomicReferenceArray<Node>[]> getPartitionMap() {
		return map;	
	}
	
	private int parseGeneration() {
		expectName(PartitionGeneration);
		
		int begin = offset;
		
		while (offset < length) {
			if (buffer[offset] == '\n') {
				String s = Buffer.utf8ToString(buffer, begin, offset - begin, sb).trim();
				offset++;
				return Integer.parseInt(s);
			}
			offset++;
		}
		throw new AerospikeException.Parse("Failed to find partition-generation value");
	}

	@SuppressWarnings("unchecked")
	private void parseReplicasMaster(Node node) {
		// Use low-level info methods and parse byte array directly for maximum performance.
		// Receive format: replicas-master\t<ns1>:<base 64 encoded bitmap1>;<ns2>:<base 64 encoded bitmap2>...\n
		expectName(ReplicasMaster);
		
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
				
				// Parse partition bitmap.
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

				AtomicReferenceArray<Node>[] replicaArray = map.get(namespace);

				if (replicaArray == null) {
					replicaArray = new AtomicReferenceArray[1];
					replicaArray[0] = new AtomicReferenceArray<Node>(partitionCount);
					copyPartitionMap();
					map.put(namespace, replicaArray);
				}

				// Log.info("Map: " + namespace + "[0] " + node);
				decodeBitmap(node, replicaArray[0], begin);
				begin = ++offset;
			}
			else {
				offset++;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void parseReplicasAll(Node node) throws AerospikeException {
		// Use low-level info methods and parse byte array directly for maximum performance.
		// Receive format: replicas-all\t
		//                 <ns1>:<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;
		//                 <ns2>:<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;\n
		expectName(ReplicasAll);
		
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
				
				// Parse replica count.
				while (offset < length) {
					byte b = buffer[offset];
					
					if (b == ',') {
						break;
					}
					offset++;
				}
				int replicaCount = Integer.parseInt(new String(buffer, begin, offset - begin));

				// Ensure replicaArray is correct size.
				AtomicReferenceArray<Node>[] replicaArray = map.get(namespace);

				if (replicaArray == null) {
					// Create new replica array. 
					replicaArray = new AtomicReferenceArray[replicaCount];
					
					for (int i = 0; i < replicaCount; i++) {
						replicaArray[i] = new AtomicReferenceArray<Node>(partitionCount);
					}

					copyPartitionMap();
					map.put(namespace, replicaArray);					
				}
				else if (replicaArray.length != replicaCount) {
					if (Log.infoEnabled()) {
						Log.info("Namespace " + namespace + " replication factor changed from " + replicaArray.length + " to " + replicaCount);
					}
					
					// Resize replica array. 
					AtomicReferenceArray<Node>[] replicaTarget = new AtomicReferenceArray[replicaCount];
					
					if (replicaArray.length < replicaCount) {
						int i = 0;
						
						// Copy existing entries.
						for (; i < replicaArray.length; i++) {
							replicaTarget[i] = replicaArray[i];
						}

						// Create new entries.
						for (; i < replicaCount; i++) {
							replicaTarget[i] = new AtomicReferenceArray<Node>(partitionCount);
						}
					}
					else {
						// Copy existing entries.
						for (int i = 0; i < replicaCount; i++) {
							replicaTarget[i] = replicaArray[i];
						}
					}
					
					copyPartitionMap();
					replicaArray = replicaTarget;
					map.put(namespace, replicaArray);				
				}
				
				// Parse partition bitmaps.
				for (int i = 0; i < replicaCount; i++) {
					begin = ++offset;

					// Find bitmap endpoint
					while (offset < length) {
						byte b = buffer[offset];
						
						if (b == ',' || b == ';') {
							break;
						}
						offset++;
					}
					
					if (offset == begin) {
						String response = getTruncatedResponse();
						throw new AerospikeException.Parse("Empty partition id for namespace " +
							namespace + ". Response=" + response);										
					}
					
					// Log.info("Map: " + namespace + '[' + i + "] " + node);
					decodeBitmap(node, replicaArray[i], begin);
				}
				begin = ++offset;
			}
			else {
				offset++;
			}
		}
	}
	
	private void decodeBitmap(Node node, AtomicReferenceArray<Node> nodeArray, int begin) {
		byte[] restoreBuffer = Base64.decode(buffer, begin, offset - begin);
	
		for (int i = 0; i < partitionCount; i++) {
			if ((restoreBuffer[i >> 3] & (0x80 >> (i & 7))) != 0) {
				// Log.info("Map: " + i);
				
				// Use lazy set because there is only one producer thread. In addition,
				// there is a one second delay due to the cluster tend polling interval.  
				// An extra millisecond for a node change will not make a difference and 
				// overall performance is improved.
				nodeArray.lazySet(i, node);
			}
		}
	}

	private void copyPartitionMap() {
		if (! copied) {
			// Make shallow copy of map.
			map = new HashMap<String,AtomicReferenceArray<Node>[]>(map);
			copied = true;
		}
		
		// Force another replicas-all request on next cluster tend.
		generation = -1;
	}

	private void expectName(String name) throws AerospikeException {
		int begin = offset;
		
		while (offset < length) {
			if (buffer[offset] == '\t') {
				String s = Buffer.utf8ToString(buffer, begin, offset - begin, sb).trim();
				
				if (name.equals(s)) {
					offset++;
					return;
				}
				break;
			}
			offset++;
		}
		throw new AerospikeException.Parse("Failed to find " + name);
	}
	
	private String getTruncatedResponse() {
		int max = (length > 200) ? 200 : length;
		return Buffer.utf8ToString(buffer, 0, max);		
	}
}
