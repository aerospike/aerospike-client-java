/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import com.aerospike.client.exp.Expression;

/**
 * Transaction policy attributes used in all database commands.
 */
public class Policy {
	/**
	 * Read policy for AP (availability) namespaces.
	 * <p>
	 * Default: {@link ReadModeAP#ONE}
	 */
	public ReadModeAP readModeAP = ReadModeAP.ONE;

	/**
	 * Read policy for SC (strong consistency) namespaces.
	 * <p>
	 * Default: {@link ReadModeSC#SESSION}
	 */
	public ReadModeSC readModeSC = ReadModeSC.SESSION;

	/**
	 * Replica algorithm used to determine the target node for a partition derived from a key
	 * or requested in a scan/query.
	 * <p>
	 * Default: {@link Replica#SEQUENCE}
	 */
	public Replica replica = Replica.SEQUENCE;

	/**
	 * Optional expression filter. If filterExp exists and evaluates to false, the
	 * transaction is ignored.
	 * <p>
	 * Default: null
	 * <p>
	 * <pre>Example:{@code
	 * Policy p = new Policy();
	 * p.filterExp = Exp.build(Exp.eq(Exp.intBin("a"), Exp.val(11)));
	 * }</pre>
	 */
	public Expression filterExp;

	/**
	 * Socket connect timeout in milliseconds. If connectTimeout greater than zero, it will
	 * be applied to creating a connection plus optional user authentication. Otherwise,
	 * socketTimeout or totalTimeout will be used depending on their values.
	 * <p>
	 * If connect, socket and total timeouts are zero, the actual socket connect timeout
	 * is hard-coded to 2000ms.
	 * <p>
	 * connectTimeout is useful when new connection creation is expensive (ie TLS connections)
	 * and it's acceptable to allow extra time to create a new connection compared to using an
	 * existing connection from the pool.
	 * <p>
	 * Default: 0
	 */
	public int connectTimeout;

	/**
	 * Socket idle timeout in milliseconds when processing a database command.
	 * <p>
	 * If socketTimeout is zero and totalTimeout is non-zero, then socketTimeout will be set
	 * to totalTimeout.  If both socketTimeout and totalTimeout are non-zero and
	 * socketTimeout &gt; totalTimeout, then socketTimeout will be set to totalTimeout. If both
	 * socketTimeout and totalTimeout are zero, then there will be no socket idle limit.
	 * <p>
	 * If socketTimeout is non-zero and the socket has been idle for at least socketTimeout,
	 * both maxRetries and totalTimeout are checked.  If maxRetries and totalTimeout are not
	 * exceeded, the transaction is retried.
	 * <p>
	 * For synchronous methods, socketTimeout is the socket timeout (SO_TIMEOUT).
	 * For asynchronous methods, the socketTimeout is implemented using a HashedWheelTimer.
	 * <p>
	 * Default: 30000ms
	 */
	public int socketTimeout = 30000;

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
	 * Default for scan/query: 0
	 * <p>
	 * Default for all other commands: 1000ms
	 */
	public int totalTimeout = 1000;

	/**
	 * Delay milliseconds after socket read timeout in an attempt to recover the socket
	 * in the background.  Processing continues on the original transaction and the user
	 * is still notified at the original transaction timeout.
	 * <p>
	 * When a transaction is stopped prematurely, the socket must be drained of all incoming
	 * data or closed to prevent unread socket data from corrupting the next transaction
	 * that would use that socket.
	 * <p>
	 * If a socket read timeout occurs and timeoutDelay is greater than zero, the socket
	 * will be drained until all data has been read or timeoutDelay is reached.  If all
	 * data has been read, the socket will be placed back into the connection pool.  If
	 * timeoutDelay is reached before the draining is complete, the socket will be closed.
	 * <p>
	 * Sync sockets are drained in the cluster tend thread at periodic intervals.  Async
	 * sockets are drained in the event loop from which the async command executed.
	 * <p>
	 * Many cloud providers encounter performance problems when sockets are closed by the
	 * client when the server still has data left to write (results in socket RST packet).
	 * If the socket is fully drained before closing, the socket RST performance penalty
	 * can be avoided on these cloud providers.
	 * <p>
	 * The disadvantage of enabling timeoutDelay is that extra memory/processing is required
	 * to drain sockets and additional connections may still be needed for transaction retries.
	 * <p>
	 * If timeoutDelay were to be enabled, 3000ms would be a reasonable value.
	 * <p>
	 * Default: 0 (no delay, connection closed on timeout)
	 */
	public int timeoutDelay;

	/**
	 * Maximum number of retries before aborting the current transaction.
	 * The initial attempt is not counted as a retry.
	 * <p>
	 * If maxRetries is exceeded, the transaction will abort with
	 * {@link com.aerospike.client.AerospikeException.Timeout}.
	 * <p>
	 * WARNING: Database writes that are not idempotent (such as add())
	 * should not be retried because the write operation may be performed
	 * multiple times if the client timed out previous transaction attempts.
	 * It's important to use a distinct WritePolicy for non-idempotent
	 * writes which sets maxRetries = 0;
	 * <p>
	 * Default for write: 0 (no retries)
	 * <p>
	 * Default for read: 2 (initial attempt + 2 retries = 3 attempts)
	 * <p>
	 * Default for scan/query: 5 (6 attempts. See {@link ScanPolicy#ScanPolicy()} comments.)
	 */
	public int maxRetries = 2;

	/**
	 * Milliseconds to sleep between retries.  Enter zero to skip sleep.
	 * This field is ignored when maxRetries is zero.
	 * This field is also ignored in async mode.
	 * <p>
	 * The sleep only occurs on connection errors and server timeouts
	 * which suggest a node is down and the cluster is reforming.
	 * The sleep does not occur when the client's socketTimeout expires.
	 * <p>
	 * Reads do not have to sleep when a node goes down because the cluster
	 * does not shut out reads during cluster reformation.  The default for
	 * reads is zero.
	 * <p>
	 * The default for writes is also zero because writes are not retried by default.
	 * Writes need to wait for the cluster to reform when a node goes down.
	 * Immediate write retries on node failure have been shown to consistently
	 * result in errors.  If maxRetries is greater than zero on a write, then
	 * sleepBetweenRetries should be set high enough to allow the cluster to
	 * reform (&gt;= 3000ms).
	 * <p>
	 * Default: 0 (do not sleep between retries)
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
	 * Use zlib compression on command buffers sent to the server and responses received
	 * from the server when the buffer size is greater than 128 bytes.
	 * <p>
	 * This option will increase cpu and memory usage (for extra compressed buffers),but
	 * decrease the size of data sent over the network.
	 * <p>
	 * Default: false
	 */
	public boolean compress;

	/**
	 * Throw exception if {@link #filterExp} is defined and that filter evaluates
	 * to false (transaction ignored).  The {@link com.aerospike.client.AerospikeException}
	 * will contain result code {@link com.aerospike.client.ResultCode#FILTERED_OUT}.
	 * <p>
	 * This field is not applicable to batch, scan or query commands.
	 * <p>
	 * Default: false
	 */
	public boolean failOnFilteredOut;

	/**
	 * Copy policy from another policy.
	 */
	public Policy(Policy other) {
		this.readModeAP = other.readModeAP;
		this.readModeSC = other.readModeSC;
		this.replica = other.replica;
		this.filterExp = other.filterExp;
		this.connectTimeout = other.connectTimeout;
		this.socketTimeout = other.socketTimeout;
		this.totalTimeout = other.totalTimeout;
		this.timeoutDelay = other.timeoutDelay;
		this.maxRetries = other.maxRetries;
		this.sleepBetweenRetries = other.sleepBetweenRetries;
		this.sendKey = other.sendKey;
		this.compress = other.compress;
		this.failOnFilteredOut = other.failOnFilteredOut;
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
	public final void setTimeout(int timeout) {
		this.socketTimeout = timeout;
		this.totalTimeout = timeout;
	}

	/**
	 * Set socketTimeout and totalTimeout.  If totalTimeout defined and
	 * socketTimeout greater than totalTimeout, set socketTimeout to
	 * totalTimeout.
	 */
	public final void setTimeouts(int socketTimeout, int totalTimeout) {
		this.socketTimeout = socketTimeout;
		this.totalTimeout = totalTimeout;

		if (totalTimeout > 0 && (socketTimeout == 0 || socketTimeout > totalTimeout)) {
			this.socketTimeout = totalTimeout;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (compress ? 1231 : 1237);
		result = prime * result + connectTimeout;
		result = prime * result + (failOnFilteredOut ? 1231 : 1237);
		result = prime * result + ((filterExp == null) ? 0 : filterExp.hashCode());
		result = prime * result + maxRetries;
		result = prime * result + ((readModeAP == null) ? 0 : readModeAP.hashCode());
		result = prime * result + ((readModeSC == null) ? 0 : readModeSC.hashCode());
		result = prime * result + ((replica == null) ? 0 : replica.hashCode());
		result = prime * result + (sendKey ? 1231 : 1237);
		result = prime * result + sleepBetweenRetries;
		result = prime * result + socketTimeout;
		result = prime * result + timeoutDelay;
		result = prime * result + totalTimeout;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Policy other = (Policy) obj;
		if (compress != other.compress)
			return false;
		if (connectTimeout != other.connectTimeout)
			return false;
		if (failOnFilteredOut != other.failOnFilteredOut)
			return false;
		if (filterExp == null) {
			if (other.filterExp != null)
				return false;
		} else if (!filterExp.equals(other.filterExp))
			return false;
		if (maxRetries != other.maxRetries)
			return false;
		if (readModeAP != other.readModeAP)
			return false;
		if (readModeSC != other.readModeSC)
			return false;
		if (replica != other.replica)
			return false;
		if (sendKey != other.sendKey)
			return false;
		if (sleepBetweenRetries != other.sleepBetweenRetries)
			return false;
		if (socketTimeout != other.socketTimeout)
			return false;
		if (timeoutDelay != other.timeoutDelay)
			return false;
		if (totalTimeout != other.totalTimeout)
			return false;
		return true;
	}
}

