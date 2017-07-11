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
	 * Replica algorithm used to determine the target node for a single record command.
	 * Batch, scan and query are not affected by replica algorithms.
	 * <p>
	 * Default: {@link Replica#SEQUENCE}
	 */
	public Replica replica = Replica.SEQUENCE;
	
	/**
	 * Socket idle timeout in milliseconds when processing a database command.
	 * <p>
	 * If socketTimeout is not zero and the socket has been idle for at least socketTimeout,
	 * both maxRetries and totalTimeout are checked.  If maxRetries and totalTimeout are not
	 * exceeded, the transaction is retried.
	 * <p>
	 * If both socketTimeout and totalTimeout are non-zero and socketTimeout > totalTimeout,
	 * then socketTimeout will be set to totalTimeout. 
	 * <p>
	 * If socketTimeout is zero, there will be no socket idle limit.
	 * <p>
	 * For synchronous methods, socketTimeout is the socket timeout (SO_TIMEOUT).
	 * For asynchronous methods, the socketTimeout is implemented using a HashedWheelTimer.
	 * <p>
	 * Default: 0 (no socket idle time limit).
	 */
	public int socketTimeout;

	/**
	 * Total transaction timeout in milliseconds.
	 * <p>
	 * The totalTimeout is tracked on the client and sent to the server along with 
	 * the transaction in the wire protocol.  The client will most likely timeout
	 * first, but the server also has the capability to timeout the transaction.
	 * <p>
	 * If totalTimeout is not zero and totalTimeout is reached before the transaction
	 * completes, the transaction will abort with
	 * {@link com.aerospike.client.AerospikeException.Timeout}.
	 * <p>
	 * If totalTimeout is zero, there will be no total time limit.
	 * <p>
	 * Default: 0 (no time limit).
	 */
	public int totalTimeout;

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
	 * The initial attempt is not counted as a retry.
	 * <p>
	 * If maxRetries is exceeded, the abort will occur with a
	 * {@link com.aerospike.client.AerospikeException.Timeout}.
	 * <p>
	 * WARNING: Database writes that are not idempotent (such as add()) 
	 * should not be retried because the write operation may be performed 
	 * multiple times if the client timed out previous transaction attempts.
	 * It's important to use a distinct WritePolicy for non-idempotent 
	 * writes which sets maxRetries = 0;
	 * <p>
	 * Default: 2 (initial attempt + 2 retries = 3 attempts)
	 */
	public int maxRetries = 2;

	/**
	 * Milliseconds to sleep between retries.  Enter zero to skip sleep.
	 * <p>
	 * The sleep only occurs on connection errors and server timeouts
	 * which suggest a node is down and the cluster is reforming.
	 * The sleep does not occur when the client's socketTimeout expires.
	 * <p>
	 * This field is ignored in async mode.
	 * <p>
	 * Reads do not have to sleep when a node goes down because the cluster
	 * does not shut out reads during cluster reformation.  The default for 
	 * reads is zero.
	 * <p>
	 * Writes need to wait for the cluster to reform when a node goes down.
	 * Immediate write retries on node failure have been shown to consistently
	 * result in errors. The default for writes is 500ms.  This default is 
	 * implemented in {@link com.aerospike.client.policy.ClientPolicy#ClientPolicy()})
	 */
	public int sleepBetweenRetries;

	/**
	 * Send user defined key in addition to hash digest on both reads and writes.
	 * If the key is sent on a write, the key will be stored with the record on 
	 * the server.
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
		this.socketTimeout = other.socketTimeout;
		this.timeoutDelay = other.timeoutDelay;
		this.maxRetries = other.maxRetries;
		this.sleepBetweenRetries = other.sleepBetweenRetries;
		this.sendKey = other.sendKey;
	}
	
	/**
	 * Default constructor.
	 */
	public Policy() {
	}
	
	/**
	 * Create a single timeout by setting socketTimeout and totalTimeout
	 * to the same value.
	 */
	public void setTimeout(int timeout) {
		this.socketTimeout = timeout;
		this.totalTimeout = timeout;
	}

	/**
	 * Set socketTimeout and totalTimeout.  If totalTimeout defined and
	 * socketTimeout greater than totalTimeout, set socketTimeout to
	 * totalTimeout.
	 */
	public void setTimeouts(int socketTimeout, int totalTimeout) {
		this.socketTimeout = socketTimeout;
		this.totalTimeout = totalTimeout;
		
		if (totalTimeout > 0 && (socketTimeout == 0 || socketTimeout > totalTimeout)) {
			this.socketTimeout = totalTimeout;
		}
	}
}
