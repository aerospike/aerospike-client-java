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

import com.aerospike.client.Key;
import com.aerospike.client.command.Buffer;

public final class Partition {
	private final String namespace;
	private final int partitionId;
	
	public Partition(Key key) {
		this.namespace = key.namespace;
		
		// CAN'T USE MOD directly - mod will give negative numbers.
		// First AND makes positive and negative correctly, then mod.
		this.partitionId = (Buffer.bytesToIntIntel(key.digest, 0) & 0xFFFF) % Node.PARTITIONS;
	}

	public Partition(String namespace, int partitionId) {
		this.namespace = namespace;
		this.partitionId = partitionId;
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
