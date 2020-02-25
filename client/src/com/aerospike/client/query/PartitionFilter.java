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
package com.aerospike.client.query;

import java.io.Serializable;

import com.aerospike.client.Key;
import com.aerospike.client.cluster.Partition;

/**
 * Partition filter used in scan/query.
 */
public final class PartitionFilter implements Serializable {
	private static final long serialVersionUID = 2L;

	/**
	 * Filter by partition id.
	 *
	 * @param id		partition id (0 - 4095)
	 */
	public static PartitionFilter id(int id) {
		return new PartitionFilter(id, 1);
	}

	/**
	 * Return records after key's digest in partition containing the digest.
	 * Note that digest order is not the same as userKey order.
	 *
	 * @param key		return records after this key's digest
	 */
	public static PartitionFilter after(Key key) {
		return new PartitionFilter(key.digest);
	}

	/**
	 * Filter by partition range.
	 *
	 * @param begin		start partition id (0 - 4095)
	 * @param count		number of partitions
	 */
	public static PartitionFilter range(int begin, int count) {
		return new PartitionFilter(begin, count);
	}

	final int begin;
	final int count;
	final byte[] digest;

	private PartitionFilter(int begin, int count) {
		this.begin = begin;
		this.count = count;
		this.digest = null;
	}

	private PartitionFilter(byte[] digest) {
		this.begin = Partition.getPartitionId(digest);
		this.count = 1;
		this.digest = digest;
	}
}
