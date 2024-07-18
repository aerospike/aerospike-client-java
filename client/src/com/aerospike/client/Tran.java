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
 * Multi-record transaction (MRT). Each command in the MRT must use the same namespace.
 */
public final class Tran {
	private final long id;
	private final ConcurrentHashMap<Key,Long> reads;
	private final Set<Key> writes;
	private String namespace;
	private int deadline;
	private boolean rollAttempted;

	/**
	 * Create MRT, assign random transaction id and initialize reads/writes hashmaps with default capacities.
	 */
	public Tran() {
		id = createId();
		reads = new ConcurrentHashMap<>();
		writes = ConcurrentHashMap.newKeySet();
	}

	/**
	 * Create MRT, assign random transaction id and initialize reads/writes hashmaps with given capacities.
	 *
	 * @param readsCapacity     expected number of record reads in the MRT. Minimum value is 16.
	 * @param writesCapacity    expected number of record writes in the MRT. Minimum value is 16.
	 */
	public Tran(int readsCapacity, int writesCapacity) {
		if (readsCapacity < 16) {
			readsCapacity = 16;
		}

		if (writesCapacity < 16) {
			writesCapacity = 16;
		}

		id = createId();
		reads = new ConcurrentHashMap<>(readsCapacity);
		writes = ConcurrentHashMap.newKeySet(writesCapacity);
	}

	private static long createId() {
		// An id of zero is considered invalid. Create random numbers
		// in a loop until non-zero is returned.
		Random r = new Random();
		long id = r.nextLong();

		while (id == 0) {
			id = r.nextLong();
		}
		return id;
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
	public void onRead(Key key, Long version) {
		// Read commands do not call setNamespace() prior to sending the command,
		// so call setNamespace() here when receiving the response.
		setNamespace(key.namespace);

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
	public void onWrite(Key key, Long version, int resultCode) {
		// Write commands call setNamespace() prior to sending the command, so there is
		// no need to call it here when receiving the response.
		if (version != null) {
			reads.put(key, version);
		}
		else {
			if (resultCode == ResultCode.OK) {
				reads.remove(key);
				writes.add(key);
			}
		}
	}

	/**
	 * Get all write keys and their versions.
	 */
	public Set<Key> getWrites() {
		return writes;
	}

	/**
	 * Return MRT namespace.
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * Set MRT namespace only if doesn't already exist.
	 * If namespace already exists, verify new namespace is the same.
	 */
	public void setNamespace(String ns) {
		if (namespace == null) {
			namespace = ns;
		}
		else if (! namespace.equals(ns)) {
			throw new AerospikeException("Namespace must be the same for all commands in the MRT. Original: " +
				namespace + " New: " + ns);
		}
	}

	/**
	 * Get MRT deadline.
	 */
	public int getDeadline() {
		return deadline;
	}

	/**
	 * Set MRT deadline. For internal use only.
	 */
	public void setDeadline(int deadline) {
		this.deadline = deadline;
	}

	/**
	 * Verify that commit/abort is only attempted once. For internal use only.
	 */
	public void setRollAttempted() {
		if (rollAttempted) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR,
				"commit() or abort() may only be called once for a given MRT");
		}
		rollAttempted = true;
	}

	/**
	 * Clear MRT. Remove all tracked keys.
	 */
	public void clear() {
		namespace = null;
		deadline = 0;
		reads.clear();
		writes.clear();
	}
}
