/*
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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
import com.aerospike.client.Log;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.util.Crypto;

/**
 * Parse node's master (and optionally prole) partitions.
 */
public final class PartitionParser extends Info {
	static final String PartitionGeneration = "partition-generation";
	static final String Replicas = "replicas";

	private HashMap<String,Partitions> map;
	private final int partitionCount;
	private final int generation;
	private boolean copied;
	private boolean regimeError;

	public PartitionParser(Connection conn, Node node, HashMap<String,Partitions> map, int partitionCount) {
		// Send format 1:  partition-generation\nreplicas\n
		super(node, conn, PartitionGeneration, Replicas);
		this.partitionCount = partitionCount;
		this.map = map;

		if (length == 0) {
			throw new AerospikeException.Parse("Partition info is empty");
		}

		this.generation = parseGeneration();
		parseReplicasAll(node, Replicas);
	}

	public int getGeneration() {
		return generation;
	}

	public boolean isPartitionMapCopied() {
		return copied;
	}

	public HashMap<String,Partitions> getPartitionMap() {
		return map;
	}

	private int parseGeneration() {
		parseName(PartitionGeneration);
		int gen = parseInt();
		expect('\n');
		return gen;
	}

	private void parseReplicasAll(Node node, String command) throws AerospikeException {
		// Use low-level info methods and parse byte array directly for maximum performance.
		// Receive format: replicas\t
		//                 <ns1>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;
		//                 <ns2>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...\n
		parseName(command);

		int begin = offset;
		int regime = 0;

		while (offset < length) {
			if (buffer[offset] == ':') {
				// Parse namespace.
				String namespace = Buffer.utf8ToString(buffer, begin, offset - begin).trim();

				if (namespace.length() <= 0 || namespace.length() >= 32) {
					String response = getTruncatedResponse();
					throw new AerospikeException.Parse("Invalid partition namespace " +
						namespace + ". Response=" + response);
				}
				begin = ++offset;

				// Parse regime.
				if (command == Replicas) {
					while (offset < length) {
						byte b = buffer[offset];

						if (b == ',') {
							break;
						}
						offset++;
					}
					regime = Integer.parseInt(new String(buffer, begin, offset - begin));
					begin = ++offset;
				}

				// Parse replica count.
				while (offset < length) {
					byte b = buffer[offset];

					if (b == ',') {
						break;
					}
					offset++;
				}
				int replicaCount = Integer.parseInt(new String(buffer, begin, offset - begin));

				// Ensure replicaCount is uniform.
				Partitions partitions = map.get(namespace);

				if (partitions == null) {
					// Create new replica array.
					partitions = new Partitions(partitionCount, replicaCount, regime != 0);
					copyPartitionMap();
					map.put(namespace, partitions);
				}
				else if (partitions.replicas.length != replicaCount) {
					if (Log.infoEnabled()) {
						Log.info("Namespace " + namespace + " replication factor changed from " + partitions.replicas.length + " to " + replicaCount);
					}

					// Resize partition map.
					Partitions tmp = new Partitions(partitions, replicaCount);

					copyPartitionMap();
					partitions = tmp;
					map.put(namespace, partitions);
				}

				// Parse partition bitmaps.
				for (int i = 0; i < replicaCount; i++) {
					begin = ++offset;

					// Find bitmap endpoint
					while (offset < length) {
						byte b = buffer[offset];

						if (b == ',' || b == ';' || b == '\n') {
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
					decodeBitmap(node, partitions, i, regime, begin);
				}
				begin = ++offset;
			}
			else {
				offset++;
			}
		}
	}

	private void decodeBitmap(Node node, Partitions partitions, int index, int regime, int begin) {
		AtomicReferenceArray<Node> nodeArray = partitions.replicas[index];
		int[] regimes = partitions.regimes;
		byte[] restoreBuffer = Crypto.decodeBase64(buffer, begin, offset - begin);

		for (int i = 0; i < partitionCount; i++) {
			Node nodeOld = nodeArray.get(i);

			if ((restoreBuffer[i >> 3] & (0x80 >> (i & 7))) != 0) {
				// Node owns this partition.
				int regimeOld = regimes[i];

				if (regime >= regimeOld) {
					// Log.info("Map: " + i);
					if (regime > regimeOld) {
						regimes[i] = regime;
					}

					if (nodeOld != null && nodeOld != node) {
						// Force previously mapped node to refresh it's partition map on next cluster tend.
						nodeOld.partitionGeneration = -1;
					}

					// Use lazy set because there is only one producer thread. In addition,
					// there is a one second delay due to the cluster tend polling interval.
					// An extra millisecond for a node change will not make a difference and
					// overall performance is improved.
					nodeArray.lazySet(i, node);
				}
				else {
					if (!regimeError) {
						if (Log.infoEnabled()) {
							Log.info(node.toString() + " regime(" + regime + ") < old regime(" + regimeOld + ")");
						}
						regimeError = true;
					}
				}
			}
		}
	}

	private void copyPartitionMap() {
		if (! copied) {
			// Make shallow copy of map.
			map = new HashMap<String,Partitions>(map);
			copied = true;
		}
	}
}
