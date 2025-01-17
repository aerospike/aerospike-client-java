/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-record transaction. Each command in the transaction must use the same namespace.
 */
public final class Txn {
	/**
	 * Transaction state.
	 */
	public static enum State {
		OPEN,
		VERIFIED,
		COMMITTED,
		ABORTED;
	}

	private static AtomicLong randomState = new AtomicLong(System.nanoTime());

	private final long id;
	private final ConcurrentHashMap<Key,Long> reads;
	private final Set<Key> writes;
	private Txn.State state;
	private String namespace;
	private int timeout;
	private int deadline;
	private boolean writeInDoubt;
	private boolean inDoubt;

	/**
	 * Create transaction, assign random transaction id and initialize reads/writes hashmaps with
	 * default capacities.
	 * <p>
	 * The default client transaction timeout is zero. This means use the server configuration
	 * mrt-duration as the transaction timeout. The default mrt-duration is 10 seconds.
	 */
	public Txn() {
		id = createId();
		reads = new ConcurrentHashMap<>();
		writes = ConcurrentHashMap.newKeySet();
		state = Txn.State.OPEN;
	}

	/**
	 * Create transaction, assign random transaction id and initialize reads/writes hashmaps with
	 * given capacities.
	 * <p>
	 * The default client transaction timeout is zero. This means use the server configuration
	 * mrt-duration as the transaction timeout. The default mrt-duration is 10 seconds.
	 *
	 * @param readsCapacity     expected number of record reads in the transaction. Minimum value is 16.
	 * @param writesCapacity    expected number of record writes in the transaction. Minimum value is 16.
	 */
	public Txn(int readsCapacity, int writesCapacity) {
		if (readsCapacity < 16) {
			readsCapacity = 16;
		}

		if (writesCapacity < 16) {
			writesCapacity = 16;
		}

		id = createId();
		reads = new ConcurrentHashMap<>(readsCapacity);
		writes = ConcurrentHashMap.newKeySet(writesCapacity);
		state = Txn.State.OPEN;
	}

	private static long createId() {
		// xorshift64* doesn't generate zeroes.
		long oldState;
		long newState;

		do {
			oldState = randomState.get();
			newState = oldState;
			newState ^= newState >>> 12;
			newState ^= newState << 25;
			newState ^= newState >>> 27;
		} while (!randomState.compareAndSet(oldState, newState));

		return newState * 0x2545f4914f6cdd1dl;
	}

	/**
	 * Return transaction ID.
	 */
	public long getId() {
		return id;
	}

	/**
	 * Set transaction timeout in seconds. The timer starts when the transaction monitor record is
	 * created. This occurs when the first command in the transaction is executed. If the timeout
	 * is reached before a commit or abort is called, the server will expire and rollback the
	 * transaction.
	 * <p>
	 * If the transaction timeout is zero, the server configuration mrt-duration is used.
	 * The default mrt-duration is 10 seconds.
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	/**
	 * Return transaction timeout in seconds.
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * Verify current transactions state and namespace for a future read command.
	 */
	void prepareRead(String ns) {
		verifyCommand();
		setNamespace(ns);
	}

	/**
	 * Verify current transaction state and namespaces for a future batch read command.
	 */
	void prepareRead(Key[] keys) {
		verifyCommand();
		setNamespace(keys);
	}

	/**
	 * Verify current transaction state and namespaces for a future batch read command.
	 */
	void prepareRead(List<BatchRead> records) {
		verifyCommand();
		setNamespace(records);
	}

	/**
	 * Verify that the transaction state allows future commands.
	 */
	public void verifyCommand() {
		if (state != Txn.State.OPEN) {
			throw new AerospikeException("Command not allowed in current transaction state: " + state);
		}
	}

	/**
	 * Process the results of a record read. For internal use only.
	 */
	public void onRead(Key key, Long version) {
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
	 * Add key to write hash when write command is in doubt (usually caused by timeout).
	 */
	public void onWriteInDoubt(Key key) {
		writeInDoubt = true;
		reads.remove(key);
		writes.add(key);
	}

	/**
	 * Get all write keys and their versions.
	 */
	public Set<Key> getWrites() {
		return writes;
	}

	/**
	 * Set transaction namespace only if doesn't already exist.
	 * If namespace already exists, verify new namespace is the same.
	 */
	public void setNamespace(String ns) {
		if (namespace == null) {
			namespace = ns;
		}
		else if (! namespace.equals(ns)) {
			throw new AerospikeException("Namespace must be the same for all commands in the transaction. orig: " +
				namespace + " new: " + ns);
		}
	}

	/**
	 * Set transaction namespaces for each key only if doesn't already exist.
	 * If namespace already exists, verify new namespace is the same.
	 */
	private void setNamespace(Key[] keys) {
		for (Key key : keys) {
			setNamespace(key.namespace);
		}
	}

	/**
	 * Set transaction namespaces for each key only if doesn't already exist.
	 * If namespace already exists, verify new namespace is the same.
	 */
	private void setNamespace(List<BatchRead> records) {
		for (BatchRead br : records) {
			setNamespace(br.key.namespace);
		}
	}

	/**
	 * Return transaction namespace.
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * Set transaction deadline. The deadline is a wall clock time calculated by the server from the
	 * transaction timeout that is sent by the client when creating the transaction monitor record.
	 * This deadline is used to avoid client/server clock skew issues. For internal use only.
	 */
	public void setDeadline(int deadline) {
		this.deadline = deadline;
	}

	/**
	 * Get transaction deadline. For internal use only.
	 */
	public int getDeadline() {
		return deadline;
	}

	/**
	 * Return if the transaction monitor record should be closed/deleted. For internal use only.
	 */
	public boolean closeMonitor() {
		return deadline != 0 && !writeInDoubt;
	}

	/**
	 * Does transaction monitor record exist.
	 */
	public boolean monitorExists() {
		return deadline != 0;
	}

	/**
	 * Set transaction state. For internal use only.
	 */
	public void setState(Txn.State state) {
		this.state = state;
	}

	/**
	 * Return transaction state.
	 */
	public Txn.State getState() {
		return state;
	}

	/**
	 * Set transaction inDoubt flag. For internal use only.
	 */
	public void setInDoubt(boolean inDoubt) {
		this.inDoubt = inDoubt;
	}

	/**
	 * Return if transaction is inDoubt.
	 */
	public boolean getInDoubt() {
		return inDoubt;
	}

	/**
	 * Clear transaction. Remove all tracked keys.
	 */
	public void clear() {
		namespace = null;
		deadline = 0;
		reads.clear();
		writes.clear();
	}
}
