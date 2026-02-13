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
 * Multi-record transaction context for grouping read and write commands into a single logical transaction.
 *
 * <p>Attach a {@code Txn} to a policy ({@link com.aerospike.client.policy.Policy#txn}) so that get/put/operate commands participate in the
 * same transaction. All commands in the transaction must use the same namespace. Commit or abort via batch APIs.
 *
 * <p>Create a transaction, attach to a write policy, perform get/put, then commit.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *     Txn txn = new Txn();
 *     WritePolicy policy = new WritePolicy(); policy.txn = txn;
 *     Key key1 = new Key("test", "set1", "id1");
 *     Key key2 = new Key("test", "set1", "id2");
 *     client.get(policy, key1);
 *     client.put(policy, key2, new Bin("name", "value"));
 *     CommitStatus status = client.commit(txn);
 * } finally { client.close(); }
 * }</pre>
 *
 * @see com.aerospike.client.policy.Policy#txn
 */
public final class Txn {
	/**
	 * Current state of the transaction.
	 */
	public static enum State {
		/** Transaction is open; reads and writes can be added. */
		OPEN,
		/** Reads have been verified; commit or abort can proceed. */
		VERIFIED,
		/** Transaction has been committed. */
		COMMITTED,
		/** Transaction has been aborted. */
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
	 * Constructs a new transaction with default capacities for read and write sets.
	 *
	 * <p>Client transaction timeout defaults to 0 (use server mrt-duration, typically 10 seconds).
	 */
	public Txn() {
		id = createId();
		reads = new ConcurrentHashMap<>();
		writes = ConcurrentHashMap.newKeySet();
		state = Txn.State.OPEN;
	}

	/**
	 * Constructs a new transaction with the given initial capacities for the read and write sets.
	 *
	 * <p>Minimum capacity for each is 16. Client transaction timeout defaults to 0 (use server mrt-duration).
	 *
	 * @param readsCapacity expected number of record reads; minimum 16
	 * @param writesCapacity expected number of record writes; minimum 16
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
	 *
	 * @return the unique transaction ID
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
	 *
	 * @param timeout timeout in seconds; 0 for server default (mrt-duration)
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	/**
	 * Return transaction timeout in seconds.
	 *
	 * @return timeout in seconds; 0 means use server mrt-duration
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
		switch (state) {
	    		case OPEN:
	        		return;
	    		case COMMITTED:
	        		throw new AerospikeException(ResultCode.TXN_ALREADY_COMMITTED,
	                		"Issuing commands to this transaction is forbidden because it has been committed.");
	    		case ABORTED:
	        		throw new AerospikeException(ResultCode.TXN_ALREADY_ABORTED,
	                		"Issuing commands to this transaction is forbidden because it has been aborted.");
	    		case VERIFIED:
	        		throw new AerospikeException(ResultCode.TXN_FAILED,
	                		"Issuing commands to this transaction is forbidden because it is currently being committed.");
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
	 *
	 * @param key the key to look up
	 * @return the read version for the key, or null if not present
	 */
	public Long getReadVersion(Key key) {
		return reads.get(key);
	}

	/**
	 * Get all read keys and their versions.
	 *
	 * @return the set of read key/version entries
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
	 *
	 * @return the set of keys written in this transaction
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
	 *
	 * @return the namespace for this transaction, or null if not yet set
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
	 *
	 * @return the server-calculated deadline
	 */
	public int getDeadline() {
		return deadline;
	}

	/**
	 * Return if the transaction monitor record should be closed/deleted. For internal use only.
	 *
	 * @return true if the monitor record should be closed
	 */
	public boolean closeMonitor() {
		return deadline != 0 && !writeInDoubt;
	}

	/**
	 * Does transaction monitor record exist.
	 *
	 * @return true if the transaction monitor record exists on the server
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
	 *
	 * @return the current transaction state (OPEN, VERIFIED, COMMITTED, or ABORTED)
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
	 *
	 * @return true if the transaction outcome is in doubt (e.g. after timeout before commit/abort)
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
