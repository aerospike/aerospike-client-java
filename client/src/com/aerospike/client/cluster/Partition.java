/*
 * Copyright 2012-2020 Aerospike, Inc.
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

public final class Partition {

	public static Partition write(Cluster cluster, Policy policy, Key key) {
		// Must copy hashmap reference for copy on write semantics to work.
		HashMap<String,Partitions> map = cluster.partitionMap;
		Partitions partitions = map.get(key.namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(key.namespace, map.size());
		}

		return new Partition(partitions, key, policy.replica, false);
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
		return new Partition(partitions, key, replica, linearize);
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

	public static Node getNodeBatchRead(Cluster cluster, Key key, Replica replica, Replica replicaSC, int sequence, int sequenceSC) {
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

		Partition p = new Partition(partitions, key, replica, false);
		p.sequence = sequence;
		return p.getNodeRead(cluster);
	}

	private final Partitions partitions;
	private final String namespace;
	private final Replica replica;
	private final int partitionId;
	private int sequence;
	private final boolean linearize;

	private Partition(Partitions partitions, Key key, Replica replica, boolean linearize) {
		this.partitions = partitions;
		this.namespace = key.namespace;
		this.replica = replica;
		this.linearize = linearize;
		this.partitionId = getPartitionId(key.digest);
	}

	public static int getPartitionId(byte[] digest) {
		// CAN'T USE MOD directly - mod will give negative numbers.
		// First AND makes positive and negative correctly, then mod.
		return (Buffer.littleBytesToInt(digest, 0) & 0xFFFF) % Node.PARTITIONS;
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
		case RANDOM:
			return getMasterNode(cluster);
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

		for (int i = 0; i < replicas.length; i++) {
			int index = Math.abs(sequence % replicas.length);
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
		Node fallback = null;
		boolean retry = (sequence > 0);

		for (int i = 1; i <= replicas.length; i++) {
			int index = Math.abs(sequence % replicas.length);
			Node node = replicas[index].get(partitionId);

			if (node != null && node.isActive()) {
				// If fallback exists, do not retry on node where command failed,
				// even if fallback is not on the same rack.
				if (retry && fallback != null && i == replicas.length) {
					return fallback;
				}

				if (node.hasRack(namespace, cluster.rackId)) {
					return node;
				}

				if (fallback == null) {
					fallback = node;
				}
			}
			sequence++;
		}

		if (fallback != null) {
			return fallback;
		}

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
		Partition other = (Partition) obj;
		return this.namespace.equals(other.namespace) && this.partitionId == other.partitionId;
	}
}
