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
	public final long trid;
	// TODO: Store namespace at top level for verification purposes?
	// TODO: set hash code for Key/digest.
	private final HashMap<Key,Long> reads;
	private final HashSet<Key> writes;

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
	public void addRead(Key key, Long version) {
		reads.put(key, version);
	}

	/**
	 * Remove record key from reads hashmap. For internal use only.
	 */
	public void removeRead(Key key) {
		reads.remove(key);
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
	 * Process the results of a single record write. For internal use only.
	 */
	public void handleWrite(Key key, Long version, int resultCode) {
		if (version != null) {
			addRead(key, version);
			removeWrite(key);
		}
		else {
			if (resultCode == ResultCode.OK) {
				removeRead(key);
			}
			else {
				removeWrite(key);
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
	 * Remove record key from writes hashmap. For internal use only.
	 */
	public void removeWrite(Key key) {
		writes.remove(key);
	}

	/**
	 * Get all write keys and their versions.
	 */
	public Set<Key> getWrites() {
		return writes;
	}

	/**
	 * Return true if a write occurred within this transaction.
	 */
	public boolean hasWrite() {
		return writes.size() > 0;
	}
}
