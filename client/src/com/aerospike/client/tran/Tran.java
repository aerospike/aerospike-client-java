/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.tran;

import com.aerospike.client.Key;
import com.aerospike.client.util.RandomShift;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Multi-record transaction.
 */
public final class Tran {
	public final long trid;
	// TODO: Store namespace at top level for verification purposes?
	// TODO: set hash code for Key/digest.
	private final HashMap<Key,Long> reads;
	private final HashSet<byte[]> writes;

	/**
	 * Create transaction with given transaction id.
	 */
	public Tran(long trid) {
		this.trid = trid;
		reads = new HashMap<>();
		writes = new HashSet<>();
	}

	/**
	 * Create transaction and assign transaction id at random.
	 */
	public Tran() {
		trid = new RandomShift().nextLong();
		reads = new HashMap<>();
		writes = new HashSet<>();
	}

	/**
	 * Add record key to reads hashmap. For internal use only.
	 */
	public void addRead(Key key, long version) {
		reads.put(key, version);
	}

	/**
	 * Add record key to writes hashmap. For internal use only.
	 */
	public void addWrite(Key key) {
		writes.add(key.digest);
	}

	/**
	 * Get all digests and their versions.
	 */
	public Long getVersion(Key key) {
		return reads.get(key);
	}

	/**
	 * Get all digests and their versions.
	 */
	public Set<Map.Entry<Key,Long>> getReads() {
		return reads.entrySet();
	}
}
