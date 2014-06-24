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

import com.aerospike.client.Key;
import com.aerospike.client.command.Buffer;

public final class Partition {
	public final String namespace;
	public final int partitionId;
	
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
