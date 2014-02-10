/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
