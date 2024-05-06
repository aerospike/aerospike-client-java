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
package com.aerospike.client;

import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.util.RandomShift;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Multi-record transaction.
 */
public final class Tran {
	public final long id;
	private final HashMap<Key,Long> reads;
	private final HashSet<Key> writes;

	/**
	 * Create transaction with given transaction id.
	 */
	public Tran(long id) {
		this.id = id;
		reads = new HashMap<>();
		writes = new HashSet<>();
	}

	/**
	 * Create transaction and assign transaction id at random.
	 */
	public Tran() {
		id = new RandomShift().nextLong();
		reads = new HashMap<>();
		writes = new HashSet<>();
	}

	/**
	 * Process the results of a record read. For internal use only.
	 */
	public void handleRead(Key key, Long version) {
		if (version != null) {
			reads.put(key, version);
		}
	}

	/**
	 * Get record version for a given key.
	 */
	public Long getReadVersion(Key key) {
		return reads.get(key);
	}

	/**
	 * Get all read keys and their versions.
	 */
	public Set<Map.Entry<Key,Long>> getReads() {
		return reads.entrySet();
	}

	/**
	 * Process the results of a record write. For internal use only.
	 */
	public void handleWrite(Key key, Long version, int resultCode) {
		if (version != null) {
			reads.put(key, version);
			writes.remove(key);
		}
		else {
			if (resultCode == ResultCode.OK) {
				reads.remove(key);
			}
			else {
				writes.remove(key);
			}
		}
	}

	/**
	 * Add record key to writes hashmap. For internal use only.
	 */
	public void addWrite(Key key) {
		writes.add(key);
	}

	/**
	 * Get all write keys and their versions.
	 */
	public Set<Key> getWrites() {
		return writes;
	}
}
