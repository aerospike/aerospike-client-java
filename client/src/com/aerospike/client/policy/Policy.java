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
package com.aerospike.client.policy;

import java.util.Objects;

import com.aerospike.client.Log;
import com.aerospike.client.Txn;
import com.aerospike.client.configuration.ConfigurationProvider;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.DynamicConfiguration;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicReadConfig;
import com.aerospike.client.exp.Expression;

/**
 * Command policy attributes used in all database commands.
 */
public class Policy {
	/**
	 * Multi-record transaction. If this field is populated, the corresponding
	 * command will be included in the transaction. This field is ignored for scan/query.
	 * <p>
	 * Default: null
	 */
	public Txn txn;

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
	 * command is ignored.
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
	 * exceeded, the command is retried.
	 * <p>
	 * For synchronous methods, socketTimeout is the socket timeout (SO_TIMEOUT).
	 * For asynchronous methods, the socketTimeout is implemented using a HashedWheelTimer.
	 * <p>
	 * Default: 30000ms
	 */
	public int socketTimeout = 30000;

	/**
	 * Total command timeout in milliseconds.
	 * <p>
	 * The totalTimeout is tracked on the client and sent to the server along with
	 * the command in the wire protocol.  The client will most likely timeout
	 * first, but the server also has the capability to timeout the command.
	 * <p>
	 * If totalTimeout is not zero and totalTimeout is reached before the command
	 * completes, the command will abort with
	 * {@link com.aerospike.client.AerospikeException.Timeout}.
	 * <p>
	 * If totalTimeout is zero, there will be no total time limit on the client side.
	 * However, the server converts zero timeouts to the server configuration field
	 * transaction-max-ms (default 1000ms) for all commands except queries. For short
	 * queries {@link QueryDuration#SHORT}, the server converts zero timeouts to a
	 * hard-coded 1000ms. For long queries, there is no timeout conversion on the
	 * server.
	 * <p>
	 * Default for scan/query: 0
	 * <p>
	 * Default for all other commands: 1000ms
	 */
	public int totalTimeout = 1000;

	/**
	 * Delay milliseconds after socket read timeout in an attempt to recover the socket
	 * in the background.  Processing continues on the original command and the user
	 * is still notified at the original command timeout.
	 * <p>
	 * When a command is stopped prematurely, the socket must be drained of all incoming
	 * data or closed to prevent unread socket data from corrupting the next command
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
	 * to drain sockets and additional connections may still be needed for command retries.
	 * <p>
	 * If timeoutDelay were to be enabled, 3000ms would be a reasonable value.
	 * <p>
	 * Default: 0 (no delay, connection closed on timeout)
	 */
	public int timeoutDelay;

	/**
	 * Maximum number of retries before aborting the current command.
	 * The initial attempt is not counted as a retry.
	 * <p>
	 * If maxRetries is exceeded, the command will abort with
	 * {@link com.aerospike.client.AerospikeException.Timeout}.
	 * <p>
	 * WARNING: Database writes that are not idempotent (such as add())
	 * should not be retried because the write operation may be performed
	 * multiple times if the client timed out previous command attempts.
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
	 * It denotes the multiplying factor to be used for exponential backoff during retries
	 * Default: 1.0, Only values greater than 1 are acceptable.
	 */
	public double sleepMultiplier = 1.0;

	/**
	 * Determine how record TTL (time to live) is affected on reads. When enabled, the server can
	 * efficiently operate as a read-based LRU cache where the least recently used records are expired.
	 * The value is expressed as a percentage of the TTL sent on the most recent write such that a read
	 * within this interval of the record’s end of life will generate a touch.
	 * <p>
	 * For example, if the most recent write had a TTL of 10 hours and read_touch_ttl_percent is set to
	 * 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
	 * recent write) will result in a touch, resetting the TTL to another 10 hours.
	 * <p>
	 * Values:
	 * <ul>
	 * <li> 0 : Use server config default-read-touch-ttl-pct for the record's namespace/set.</li>
	 * <li>-1 : Do not reset record TTL on reads.</li>
	 * <li>1 - 100 : Reset record TTL on reads when within this percentage of the most recent write TTL.</li>
	 * </ul>
	 * <li>
	 * <p>
	 * Default: 0
	 */
	public int readTouchTtlPercent;

	/**
	 * Send user defined key in addition to hash digest on both reads and writes.
	 * If the key is sent on a write, the key will be stored with the record on
	 * the server.
	 * <p>
	 * If the key is sent on a read, the server will generate the hash digest from
	 * the key and validate that digest with the digest sent by the client. Unless
	 * this is the explicit intent of the developer, avoid sending the key on reads.
	 * <p>
	 * Default: false (do not send the user defined key)
	 */
	public boolean sendKey;

	/**
	 * Use zlib compression on command buffers sent to the server and responses received
	 * from the server when the buffer size is greater than 128 bytes. This option will
	 * increase cpu and memory usage (for extra compressed buffers), but decrease the size
	 * of data sent over the network.
	 * <p>
	 * This compression feature requires the Enterprise Edition Server.
	 * <p>
	 * Default: false
	 */
	public boolean compress;

	/**
	 * Throw exception if {@link #filterExp} is defined and that filter evaluates
	 * to false (command ignored).  The {@link com.aerospike.client.AerospikeException}
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
		this.txn = other.txn;
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
		this.readTouchTtlPercent = other.readTouchTtlPercent;
		this.sendKey = other.sendKey;
		this.compress = other.compress;
		this.failOnFilteredOut = other.failOnFilteredOut;
		this.sleepMultiplier = other.sleepMultiplier;
	}

	/**
	 * Copy policy from another policy AND override certain policy attributes if they exist in the configProvider.
	 * Any policy overrides will not get logged.
	 */
	public Policy(Policy other, ConfigurationProvider configProvider) {
		this(other);
		updateFromConfig(configProvider,false);
	}

	/**
	 * Copy policy from another policy AND override certain policy attributes if they exist in the configProvider.
	 * Any default policy overrides will get logged.
	 */
	public Policy(Policy other, ConfigurationProvider configProvider, boolean isDefaultPolicy) {
		this(other);
		updateFromConfig(configProvider, isDefaultPolicy);
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

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setTxn(Txn txn) {
		this.txn = txn;
	}

	public void setReadModeAP(ReadModeAP readModeAP) {
		this.readModeAP = readModeAP;
	}

	public void setReadModeSC(ReadModeSC readModeSC) {
		this.readModeSC = readModeSC;
	}

	public void setReplica(Replica replica) {
		this.replica = replica;
	}

	public void setFilterExp(Expression filterExp) {
		this.filterExp = filterExp;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public void setTotalTimeout(int totalTimeout) {
		this.totalTimeout = totalTimeout;
	}

	public void setTimeoutDelay(int timeoutDelay) {
		this.timeoutDelay = timeoutDelay;
	}

	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}

	public void setSleepBetweenRetries(int sleepBetweenRetries) {
		this.sleepBetweenRetries = sleepBetweenRetries;
	}

	public void setReadTouchTtlPercent(int readTouchTtlPercent) {
		this.readTouchTtlPercent = readTouchTtlPercent;
	}

	public void setSendKey(boolean sendKey) {
		this.sendKey = sendKey;
	}

	public void setCompress(boolean compress) {
		this.compress = compress;
	}

	public void setFailOnFilteredOut(boolean failOnFilteredOut) {
		this.failOnFilteredOut = failOnFilteredOut;
	}

	public void setSleepMultiplier(double sleepMultiplier) { this.sleepMultiplier = sleepMultiplier; }

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Policy policy = (Policy) o;
		return connectTimeout == policy.connectTimeout && socketTimeout == policy.socketTimeout && totalTimeout == policy.totalTimeout && timeoutDelay == policy.timeoutDelay && maxRetries == policy.maxRetries && sleepBetweenRetries == policy.sleepBetweenRetries && Double.compare(policy.sleepMultiplier, sleepMultiplier) == 0 && readTouchTtlPercent == policy.readTouchTtlPercent && sendKey == policy.sendKey && compress == policy.compress && failOnFilteredOut == policy.failOnFilteredOut && Objects.equals(txn, policy.txn) && readModeAP == policy.readModeAP && readModeSC == policy.readModeSC && replica == policy.replica && Objects.equals(filterExp, policy.filterExp);
	}

	@Override
	public int hashCode() {
		return Objects.hash(txn, readModeAP, readModeSC, replica, filterExp, connectTimeout, socketTimeout, totalTimeout, timeoutDelay, maxRetries, sleepBetweenRetries, sleepMultiplier, readTouchTtlPercent, sendKey, compress, failOnFilteredOut);
	}

	private void updateFromConfig(ConfigurationProvider configProvider, boolean log) {
		boolean logUpdate = false;
		if (configProvider == null) {
			return;
		}
		Configuration config = configProvider.fetchConfiguration();
		if (config == null) {
			return;
		}
		DynamicConfiguration dConfig = config.getDynamicConfiguration();
		if (dConfig == null) {
			return;
		}
		DynamicReadConfig dynRC = dConfig.getDynamicReadConfig();
		if (dynRC == null) {
			return;
		}

		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}
		if (dynRC.readModeAP != null && this.readModeAP != dynRC.readModeAP) {
			this.readModeAP = dynRC.readModeAP;
			if (logUpdate) {
				Log.info("Set Policy.readModeAP = " + this.readModeAP);
			}
		}
		if (dynRC.readModeSC != null && this.readModeSC != dynRC.readModeSC) {
			this.readModeSC = dynRC.readModeSC;
			if (logUpdate) {
				Log.info("Set Policy.readModeSC = " + this.readModeSC);
			}
		}
		if (dynRC.connectTimeout != null && this.connectTimeout != dynRC.connectTimeout.value) {
			this.connectTimeout = dynRC.connectTimeout.value;
			if (logUpdate) {
				Log.info("Set Policy.connectTimeout = " + this.connectTimeout);
			}
		}
		if (dynRC.failOnFilteredOut != null && this.failOnFilteredOut != dynRC.failOnFilteredOut.value) {
			this.failOnFilteredOut = dynRC.failOnFilteredOut.value;
			if (logUpdate) {
				Log.info("Set Policy.failOnFilteredOut = " + this.failOnFilteredOut);
			}
		}
		if (dynRC.replica != null && this.replica != dynRC.replica) {
			this.replica = dynRC.replica;
			if (logUpdate) {
				Log.info("Set Policy.replica = " + this.replica);
			}
		}
		if (dynRC.sleepBetweenRetries != null && this.sleepBetweenRetries != dynRC.sleepBetweenRetries.value) {
			this.sleepBetweenRetries = dynRC.sleepBetweenRetries.value;
			if (logUpdate) {
				Log.info("Set Policy.sleepBetweenRetries = " + this.sleepBetweenRetries);
			}
		}
		if (dynRC.sleepMultiplier != null && this.sleepMultiplier != dynRC.sleepMultiplier.value) {
			this.sleepMultiplier = dynRC.sleepMultiplier.value;
			if (logUpdate) {
				Log.info("Set Policy.sleepMultiplier = " + this.sleepMultiplier);
			}
		}
		if (dynRC.socketTimeout != null && this.socketTimeout != dynRC.socketTimeout.value) {
			this.socketTimeout = dynRC.socketTimeout.value;
			if (logUpdate) {
				Log.info("Set Policy.socketTimeout = " + this.socketTimeout);
			}
		}
		if (dynRC.timeoutDelay != null && this.timeoutDelay != dynRC.timeoutDelay.value) {
			this.timeoutDelay = dynRC.timeoutDelay.value;
			if (logUpdate) {
				Log.info("Set Policy.timeoutDelay = " + this.timeoutDelay);
			}
		}
		if (dynRC.totalTimeout != null && this.totalTimeout != dynRC.totalTimeout.value) {
			this.totalTimeout = dynRC.totalTimeout.value;
			if (logUpdate) {
				Log.info("Set Policy.totalTimeout = " + this.totalTimeout);
			}
		}
		if (dynRC.maxRetries != null && this.maxRetries != dynRC.maxRetries.value) {
			this.maxRetries = dynRC.maxRetries.value;
			if (logUpdate) {
				Log.info("Set Policy.maxRetries = " + this.maxRetries);
			}
		}
	}
}
