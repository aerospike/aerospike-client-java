/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.client.policy;

/**
 * Container object for transaction policy attributes used in all database
 * operation calls.
 */
public class Policy {
	/**
	 * Transaction policy attributes used in all database commands.
	 */
	public Priority priority = Priority.DEFAULT;
	
	/**
	 * How replicas should be consulted in a read operation to provide the desired
	 * consistency guarantee.
	 * <p>
	 * Default: {@link ConsistencyLevel#CONSISTENCY_ONE}
	 */
	public ConsistencyLevel consistencyLevel = ConsistencyLevel.CONSISTENCY_ONE;

	/**
	 * Send read commands to the node containing the key's partition replica type.
	 * Write commands are not affected by this setting, because all writes are directed 
	 * to the node containing the key's master partition.
	 * <p>
	 * Default: {@link Replica#MASTER}
	 */
	public Replica replica = Replica.MASTER;
	
	/**
	 * Total transaction timeout in milliseconds for both client and server.
	 * The timeout is tracked on the client and also sent to the server along 
	 * with the transaction in the wire protocol.  The client will most likely
	 * timeout first, but the server has the capability to timeout the transaction
	 * as well.
	 * <p>
	 * The timeout is also used as a socket timeout.
	 * Default: 0 (no timeout).
	 */
	public int timeout;
	
	/**
	 * Delay milliseconds after transaction timeout before closing socket in async mode only.
	 * When a transaction is stopped prematurely, the socket must be closed and not placed back
	 * on the pool. This is done to prevent unread socket data from corrupting the next transaction
	 * that would use that socket.
	 * <p>
	 * This field delays the closing of the socket to give the transaction more time to complete
	 * in the hope that the socket can be reused.  This is helpful when timeouts are aggressive 
	 * and a certain percentage of timeouts is expected.
	 * <p>
	 * The user is still notified of the timeout in async mode at the original timeout value.
	 * The transaction's async timer is then reset to this delay and the transaction is allowed
	 * to continue.  If the transactions succeeds within the delay, then the socket is placed back
	 * on the pool and the transaction response is discarded.  Otherwise, the socket must be closed.
	 * <p>
	 * This field is ignored in sync mode because control must be returned back to user on timeout 
	 * and there is no currently available thread pool to process the delay.
	 * <p>
	 * Default: 0 (no delay, connection closed on timeout)
	 */
	public int timeoutDelay;

	/**
	 * Maximum number of retries before aborting the current transaction.
	 * A retry may be attempted when there is a network error.  
	 * If maxRetries is exceeded, the abort will occur even if the timeout 
	 * has not yet been exceeded.
	 * <p>
	 * Default: 1
	 */
	public int maxRetries = 1;

	/**
	 * Milliseconds to sleep between retries.  Enter zero to skip sleep.
	 * This field is ignored in async mode.
	 * <p>
	 * Default: 500ms
	 */
	public int sleepBetweenRetries = 500;
	
	/**
	 * Should the client retry a command if the timeout is reached.
	 * <p>
	 * If false, throw timeout exception when the timeout has been reached.  Note that
	 * retries can still occur if a command fails on a network error before the timeout
	 * has been reached.
	 * <p>
	 * If true, retry command with same timeout when the timeout has been reached.
	 * The maximum number of retries is defined by maxRetries.  Note that retries in 
	 * async mode can only be made if timeoutDelay is zero. Otherwise, deadlock would 
	 * have been possible.
	 * <p>
	 * Default: false
	 */
	public boolean retryOnTimeout;

	/**
	 * Send user defined key in addition to hash digest on both reads and writes.
	 * <p>
	 * Default: false (do not send the user defined key)
	 */
	public boolean sendKey;
	
	/**
	 * Copy policy from another policy.
	 */
	public Policy(Policy other) {
		this.priority = other.priority;
		this.consistencyLevel = other.consistencyLevel;
		this.replica = other.replica;
		this.timeout = other.timeout;
		this.timeoutDelay = other.timeoutDelay;
		this.maxRetries = other.maxRetries;
		this.sleepBetweenRetries = other.sleepBetweenRetries;
		this.retryOnTimeout = other.retryOnTimeout;
		this.sendKey = other.sendKey;
	}
	
	/**
	 * Default constructor.
	 */
	public Policy() {
	}
}
