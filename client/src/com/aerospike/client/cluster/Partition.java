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
import com.aerospike.client.Key;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.query.PartitionStatus;

public final class Partition {

	public static Partition write(Cluster cluster, Policy policy, Key key) {
		// Must copy hashmap reference for copy on write semantics to work.
		HashMap<String,Partitions> map = cluster.partitionMap;
		Partitions partitions = map.get(key.namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(key.namespace, map.size());
		}

		return new Partition(partitions, key, policy.replica, null, false);
	}

	public static Partition read(Cluster cluster, Policy policy, Key key) {
		// Must copy hashmap reference for copy on write semantics to work.
		HashMap<String,Partitions> map = cluster.partitionMap;
		Partitions partitions = map.get(key.namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(key.namespace, map.size());
		}

		Replica replica;
		boolean linearize;

		if (partitions.scMode) {
			switch (policy.readModeSC) {
			case SESSION:
				replica = Replica.MASTER;
				linearize = false;
				break;

			case LINEARIZE:
				replica = policy.replica == Replica.PREFER_RACK ? Replica.SEQUENCE : policy.replica;
				linearize = true;
				break;

			default:
				replica = policy.replica;
				linearize = false;
				break;
			}
		}
		else {
			replica = policy.replica;
			linearize = false;
		}
		return new Partition(partitions, key, replica, null, linearize);
	}

	public static Replica getReplicaSC(Policy policy) {
		switch (policy.readModeSC) {
		case SESSION:
			return Replica.MASTER;

		case LINEARIZE:
			return policy.replica == Replica.PREFER_RACK ? Replica.SEQUENCE : policy.replica;

		default:
			return policy.replica;
		}
	}

	public static Node getNodeBatchWrite(
		Cluster cluster,
		Key key,
		Replica replica,
		Node prevNode,
		int sequence
	) {
		HashMap<String,Partitions> map = cluster.partitionMap;
		Partitions partitions = map.get(key.namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(key.namespace, map.size());
		}

		Partition p = new Partition(partitions, key, replica, prevNode, false);
		p.sequence = sequence;
		return p.getNodeWrite(cluster);
	}

	public static Node getNodeBatchRead(
		Cluster cluster,
		Key key,
		Replica replica,
		Replica replicaSC,
		Node prevNode,
		int sequence,
		int sequenceSC
	) {
		// Must copy hashmap reference for copy on write semantics to work.
		HashMap<String,Partitions> map = cluster.partitionMap;
		Partitions partitions = map.get(key.namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(key.namespace, map.size());
		}

		if (partitions.scMode) {
			replica = replicaSC;
			sequence = sequenceSC;
		}

		Partition p = new Partition(partitions, key, replica, prevNode, false);
		p.sequence = sequence;
		return p.getNodeRead(cluster);
	}

	private Partitions partitions;
	private final String namespace;
	private final Replica replica;
	public Node prevNode;
	private int partitionId;
	public int sequence;
	private final boolean linearize;

	private Partition(Partitions partitions, Key key, Replica replica, Node prevNode, boolean linearize) {
		this.partitions = partitions;
		this.namespace = key.namespace;
		this.replica = replica;
		this.prevNode = prevNode;
		this.linearize = linearize;
		this.partitionId = getPartitionId(key.digest);
	}

	public Partition(String namespace, Replica replica) {
		this.namespace = namespace;
		this.replica = replica;
		this.linearize = false;
	}

	public static int getPartitionId(byte[] digest) {
		// CAN'T USE MOD directly - mod will give negative numbers.
		// First AND makes positive and negative correctly, then mod.
		return (Buffer.littleBytesToInt(digest, 0) & 0xFFFF) % Node.PARTITIONS;
	}

	public Node getNodeQuery(Cluster cluster, Partitions partitions, PartitionStatus ps) {
		this.partitions = partitions;
		this.partitionId = ps.id;
		this.sequence = ps.sequence;
		this.prevNode = ps.node;

		Node node = getNodeRead(cluster);
		ps.node = node;
		ps.sequence = this.sequence;
		ps.retry = false;
		return node;
	}

	public Node getNodeRead(Cluster cluster) {
		switch (replica) {
		default:
		case SEQUENCE:
			return getSequenceNode(cluster);

		case PREFER_RACK:
			return getRackNode(cluster);

		case MASTER:
			return getMasterNode(cluster);

		case MASTER_PROLES:
			return getMasterProlesNode(cluster);

		case RANDOM:
			return cluster.getRandomNode();
		}
	}

	public Node getNodeWrite(Cluster cluster) {
		switch (replica) {
		default:
		case SEQUENCE:
		case PREFER_RACK:
			return getSequenceNode(cluster);

		case MASTER:
		case MASTER_PROLES:
			return getMasterNode(cluster);
		case RANDOM:
			return cluster.getRandomNode();
		}
	}

	public void prepareRetryRead(boolean timeout) {
		if (! timeout || !linearize) {
			sequence++;
		}
	}

	public void prepareRetryWrite(boolean timeout) {
		if (! timeout) {
			sequence++;
		}
	}

	private Node getSequenceNode(Cluster cluster) {
		AtomicReferenceArray<Node>[] replicas = partitions.replicas;
		int max = replicas.length;

		for (int i = 0; i < max; i++) {
			int index = sequence % max;
			Node node = replicas[index].get(partitionId);

			if (node != null && node.isActive()) {
				return node;
			}
			sequence++;
		}
		Node[] nodeArray = cluster.getNodes();
		throw new AerospikeException.InvalidNode(nodeArray.length, this);
	}

	private Node getRackNode(Cluster cluster) {
		AtomicReferenceArray<Node>[] replicas = partitions.replicas;
		int max = replicas.length;
		int seq1 = 0;
		int seq2 = 0;
		Node fallback1 = null;
		Node fallback2 = null;
		int[] rackIds = cluster.rackIds;

		for (int rackId : rackIds) {
			int seq = sequence;

			for (int i = 0; i < max; i++) {
				int index = seq % max;
				Node node = replicas[index].get(partitionId);
				// Log.info("Try " + rackId + ',' + index + ',' + prevNode + ',' + node + ',' + node.hasRack(namespace, rackId));

				if (node != null) {
					// Avoid retrying on node where command failed
					// even if node is the only one on the same rack.
					if (node != prevNode) {
						if (node.hasRack(namespace, rackId)) {
							if (node.isActive()) {
								// Log.info("Found node on same rack: " + node);
								prevNode = node;
								sequence = seq;
								return node;
							}
						}
						else if (fallback1 == null && node.isActive()) {
							// Meets all criteria except not on same rack.
							fallback1 = node;
							seq1 = seq;
						}
					}
					else if (fallback2 == null && node.isActive()){
						// Previous node is the least desirable fallback.
						fallback2 = node;
						seq2 = seq;
					}
				}
				seq++;
			}
		}

		// Return node on a different rack if it exists.
		if (fallback1 != null) {
			// Log.info("Found fallback node: " + fallback1);
			prevNode = fallback1;
			sequence = seq1;
			return fallback1;
		}

		// Return previous node if it still exists.
		if (fallback2 != null) {
			// Log.info("Found previous node: " + fallback2);
			prevNode = fallback2;
			sequence = seq2;
			return fallback2;
		}

		// Failed to find suitable node.
		Node[] nodeArray = cluster.getNodes();
		throw new AerospikeException.InvalidNode(nodeArray.length, this);
	}

	private Node getMasterNode(Cluster cluster) {
		Node node = partitions.replicas[0].get(partitionId);

		if (node != null && node.isActive()) {
			return node;
		}
		Node[] nodeArray = cluster.getNodes();
		throw new AerospikeException.InvalidNode(nodeArray.length, this);
	}

	private Node getMasterProlesNode(Cluster cluster) {
		AtomicReferenceArray<Node>[] replicas = partitions.replicas;

		for (int i = 0; i < replicas.length; i++) {
			int index = Math.abs(cluster.replicaIndex.getAndIncrement() % replicas.length);
			Node node = replicas[index].get(partitionId);

			if (node != null && node.isActive()) {
				return node;
			}
		}
		Node[] nodeArray = cluster.getNodes();
		throw new AerospikeException.InvalidNode(nodeArray.length, this);
	}

	@Override
	public String toString() {
		return namespace + ':' + partitionId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = prime + namespace.hashCode();
		result = prime * result + partitionId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		Partition other = (Partition) obj;
		return this.namespace.equals(other.namespace) && this.partitionId == other.partitionId;
	}
}
