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

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-record transaction.
 */
public final class Tran {
	private long id;
	private String namespace;
	private final ConcurrentHashMap<Key,Long> reads;
	private final Set<Key> writes;

	/**
	 * Create transaction with given transaction id.
	 */
	public Tran(long id) {
		if (id == 0) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "MRT id must be non-zero");
		}
		this.id = id;
		reads = new ConcurrentHashMap<>();
		writes = ConcurrentHashMap.newKeySet();
	}

	/**
	 * Create transaction and assign transaction id at random.
	 */
	public Tran() {
		// An id of zero is considered invalid. Create random numbers
		// in a loop until non-zero is returned.
		Random r = new Random();
		long v = r.nextLong();

		while (v == 0) {
			v = r.nextLong();
		}
		id = v;
		reads = new ConcurrentHashMap<>();
		writes = ConcurrentHashMap.newKeySet();
	}

	/**
	 * Return MRT ID.
	 */
	public long getId() {
		return id;
	}

	/**
	 * Process the results of a record read. For internal use only.
	 */
	public void handleRead(Key key, Long version) {
		if (version != null) {
			verifyNamespace(key);
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
			verifyNamespace(key);
			reads.put(key, version);
		}
		else {
			if (resultCode == ResultCode.OK) {
				verifyNamespace(key);
				reads.remove(key);
				writes.add(key);
			}
		}
	}

	private void verifyNamespace(Key key) {
		if (namespace == null) {
			namespace = key.namespace;
		}
		else if (! namespace.equals(key.namespace)) {
			throw new AerospikeException("Namespace must be the same for all commands in the MRT. Expected: " +
				namespace + " Received: " + key.namespace);
		}
	}

	/**
	 * Get all write keys and their versions.
	 */
	public Set<Key> getWrites() {
		return writes;
	}

	/**
	 * Close transaction. Remove all tracked keys.
	 */
	public void close() {
		reads.clear();
		writes.clear();
	}
}
