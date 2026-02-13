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

import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.Policy;

import java.util.List;

/**
 * Base exception thrown by the Aerospike client when a database command fails or a client-side error occurs.
 *
 * <p>Subclasses represent specific failure modes (e.g. {@link Timeout}, {@link Connection},
 * {@link InvalidNode}). Use {@link #getResultCode()} to get the numeric result code and
 * {@link #getBaseMessage()} for the message without connection metadata.
 *
 * <p>Handle Timeout and generic AerospikeException; use getResultCode() and getInDoubt().</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *     client.get(policy, key);
 * } catch (AerospikeException.Timeout e) {
 *     // Client or server timeout
 *     Policy p = e.getPolicy();
 *     log.warn("Timeout after {} ms", p != null ? p.totalTimeout : 0);
 * } catch (AerospikeException e) {
 *     int code = e.getResultCode();
 *     if (e.getInDoubt()) {
 *         // Write may have completed; decide whether to retry
 *     }
 *     throw e;
 * }
 * }</pre>
 *
 * @see #getResultCode()
 * @see #getBaseMessage()
 * @see ResultCode
 */
public class AerospikeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	protected transient Node node;
	protected transient Policy policy;
	protected List<AerospikeException> subExceptions;
	protected int resultCode = ResultCode.CLIENT_ERROR;
	protected int iteration = -1;
	protected boolean inDoubt;

	/**
	 * Constructs an exception with the given result code and message.
	 *
	 * @param resultCode the Aerospike result code (e.g. from {@link ResultCode})
	 * @param message the detail message; may be {@code null}
	 */
	public AerospikeException(int resultCode, String message) {
		super(message);
		this.resultCode = resultCode;
	}

	/**
	 * Constructs an exception with the given result code and cause.
	 *
	 * @param resultCode the Aerospike result code (e.g. from {@link ResultCode})
	 * @param e the cause; may be {@code null}
	 */
	public AerospikeException(int resultCode, Throwable e) {
		super(e);
		this.resultCode = resultCode;
	}

	/**
	 * Constructs an exception with the given result code and no message.
	 *
	 * @param resultCode the Aerospike result code (e.g. from {@link ResultCode})
	 */
	public AerospikeException(int resultCode) {
		super();
		this.resultCode = resultCode;
	}

	/**
	 * Constructs an exception with the given result code and in-doubt flag.
	 *
	 * @param resultCode the Aerospike result code (e.g. from {@link ResultCode})
	 * @param inDoubt whether the write may have completed on the server
	 */
	public AerospikeException(int resultCode, boolean inDoubt) {
		super();
		this.resultCode = resultCode;
		this.inDoubt = inDoubt;
	}

	/**
	 * Constructs an exception with the given result code, message, and cause.
	 *
	 * @param resultCode the Aerospike result code (e.g. from {@link ResultCode})
	 * @param message the detail message; may be {@code null}
	 * @param e the cause; may be {@code null}
	 */
	public AerospikeException(int resultCode, String message, Throwable e) {
		super(message, e);
		this.resultCode = resultCode;
	}

	/**
	 * Constructs an exception with the given message and cause (result code remains {@link ResultCode#CLIENT_ERROR}).
	 *
	 * @param message the detail message; may be {@code null}
	 * @param e the cause; may be {@code null}
	 */
	public AerospikeException(String message, Throwable e) {
		super(message, e);
	}

	/**
	 * Constructs an exception with the given message (result code remains {@link ResultCode#CLIENT_ERROR}).
	 *
	 * @param message the detail message; may be {@code null}
	 */
	public AerospikeException(String message) {
		super(message);
	}

	/**
	 * Constructs an exception with the given cause (result code remains {@link ResultCode#CLIENT_ERROR}).
	 *
	 * @param e the cause; may be {@code null}
	 */
	public AerospikeException(Throwable e) {
		super(e);
	}

	@Override
	public String getMessage() {
		StringBuilder sb = new StringBuilder(512);

		sb.append("Error ");
		sb.append(resultCode);

		if (iteration >= 0) {
			sb.append(',');
			sb.append(iteration);
		}

		if (policy != null) {
			sb.append(',');
			sb.append(policy.connectTimeout);
			sb.append(',');
			sb.append(policy.socketTimeout);
			sb.append(',');
			sb.append(policy.totalTimeout);
			sb.append(',');
			sb.append(policy.maxRetries);
		}

		if (inDoubt) {
			sb.append(",inDoubt");
		}

		if (node != null) {
			sb.append(',');
			sb.append(node.toString());
		}

		sb.append(": ");
		sb.append(getBaseMessage());

		if (subExceptions != null) {
			sb.append(System.lineSeparator());
			sb.append("sub-exceptions:");

			for (AerospikeException ae : subExceptions) {
				sb.append(System.lineSeparator());
				sb.append(ae.getMessage());
			}
		}
		return sb.toString();
	}

	/**
	 * Returns the exception message without connection or policy metadata.
	 *
	 * <p>Use this for user-facing or log messages when the full {@link #getMessage()} output
	 * is too verbose.
	 *
	 * @return the base message, or the result code string from {@link ResultCode} if the message is {@code null}
	 */
	public String getBaseMessage() {
		String message = super.getMessage();
		return (message != null)? message : ResultCode.getResultString(resultCode);
	}

	/**
	 * Indicates whether the connection that was in use for the failed command should be returned to the pool.
	 *
	 * <p>Some result codes indicate a bad connection; in those cases this returns {@code false}.
	 *
	 * @return {@code true} if the connection can be reused, {@code false} if it should be discarded
	 */
	public final boolean keepConnection() {
		return ResultCode.keepConnection(resultCode);
	}

	/**
	 * Returns the cluster node that was used for the command when the exception occurred.
	 *
	 * @return the node, or {@code null} if not set
	 */
	public final Node getNode() {
		return node;
	}

	/**
	 * Sets the cluster node associated with this exception.
	 *
	 * @param node the node; may be {@code null}
	 */
	public final void setNode(Node node) {
		this.node = node;
	}

	/**
	 * Returns the policy used for the command that failed, when applicable.
	 *
	 * @return the policy, or {@code null} for non-command exceptions
	 */
	public final Policy getPolicy() {
		return policy;
	}

	/**
	 * Sets the policy associated with this exception.
	 *
	 * @param policy the policy; may be {@code null}
	 */
	public final void setPolicy(Policy policy) {
		this.policy = policy;
	}

	/**
	 * Returns the list of exceptions from individual retry attempts, when retries were performed.
	 *
	 * @return the list of sub-exceptions, or {@code null} if no retry occurred
	 */
	public final List<AerospikeException> getSubExceptions() {
		return subExceptions;
	}

	/**
	 * Sets the list of exceptions from individual retry attempts.
	 *
	 * @param subExceptions the list of sub-exceptions; may be {@code null}
	 */
	public final void setSubExceptions(List<AerospikeException> subExceptions) {
		this.subExceptions = subExceptions;
	}

	/**
	 * Returns the Aerospike result code for this exception.
	 *
	 * @return the result code (e.g. from {@link ResultCode})
	 */
	public final int getResultCode() {
		return resultCode;
	}

	/**
	 * Returns the number of command attempts made before the failure.
	 *
	 * @return the attempt count, or -1 if not set
	 */
	public final int getIteration() {
		return iteration;
	}

	/**
	 * Sets the number of command attempts made before the failure.
	 *
	 * @param iteration the attempt count (e.g. 1 for first attempt)
	 */
	public final void setIteration(int iteration) {
		this.iteration = iteration;
	}

	/**
	 * Indicates whether the write command may have completed on the server despite this exception.
	 *
	 * <p>When {@code true}, a timeout or connection error may have occurred after the write was
	 * applied; callers should decide whether to retry or treat as success based on idempotency.
	 *
	 * @return {@code true} if the write might have completed, {@code false} otherwise
	 */
	public final boolean getInDoubt() {
		return inDoubt;
	}

	/**
	 * Sets the in-doubt flag based on whether this was a write and whether the command was sent.
	 *
	 * <p>The in-doubt flag is set when a client error (e.g. timeout) occurs after the command
	 * was sent to the server, so the write may have completed.
	 *
	 * @param isWrite {@code true} if the command was a write
	 * @param commandSentCounter number of times the command was sent (e.g. 1 for single send)
	 */
	public final void setInDoubt(boolean isWrite, int commandSentCounter) {
		if (isWrite && (commandSentCounter > 1 || (commandSentCounter == 1 && (resultCode == ResultCode.TIMEOUT || resultCode <= 0)))) {
			this.inDoubt = true;
		}
	}

	/**
	 * Sets whether the write command may have completed on the server.
	 *
	 * @param inDoubt {@code true} if the write might have completed, {@code false} otherwise
	 */
	public void setInDoubt(boolean inDoubt) {
		this.inDoubt = inDoubt;
	}

	/**
	 * Thrown when a database request does not complete within the configured timeout.
	 *
	 * <p>Use {@link #client} to distinguish client-side timeouts from server-initiated timeouts.
	 * The policy timeouts are available via {@link #getPolicy()} or the redundant fields
	 * {@link #connectTimeout}, {@link #socketTimeout}, and {@link #timeout} for backward compatibility.
	 *
	 * @see #getPolicy()
	 */
	public static final class Timeout extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Socket initial connect timeout in milliseconds. Redundant with {@link Policy#connectTimeout};
		 * retained for backward compatibility.
		 */
		public int connectTimeout;

		/**
		 * Socket idle timeout in milliseconds. Redundant with {@link Policy#socketTimeout};
		 * retained for backward compatibility.
		 */
		public int socketTimeout;

		/**
		 * Total timeout in milliseconds. Redundant with {@link Policy#totalTimeout};
		 * retained for backward compatibility.
		 */
		public int timeout;

		/**
		 * {@code true} if the client enforced the timeout; {@code false} if the server reported a timeout.
		 */
		public boolean client;

		/**
		 * Constructs a client timeout with the given message and metadata.
		 *
		 * @param message the detail message; may be {@code null}
		 * @param iteration the attempt count
		 * @param totalTimeout total timeout in milliseconds
		 * @param inDoubt whether the write may have completed
		 */
		public Timeout(String message, int iteration, int totalTimeout, boolean inDoubt) {
			super(ResultCode.TIMEOUT, message);
			super.iteration = iteration;
			super.inDoubt = inDoubt;

			Policy p = new Policy();
			p.connectTimeout = 0;
			p.socketTimeout = 0;
			p.totalTimeout = totalTimeout;
			p.maxRetries = -1;
			super.policy = p;

			this.connectTimeout = 0;
			this.socketTimeout = 0;
			this.timeout = totalTimeout;
			this.client = true;
		}

		/**
		 * Constructs a timeout with the policy that was used and whether it was client or server initiated.
		 *
		 * @param policy the command policy; used to populate timeout fields
		 * @param client {@code true} for client timeout, {@code false} for server timeout
		 */
		public Timeout(Policy policy, boolean client) {
			// Other base exception fields are set after this constructor.
			super(ResultCode.TIMEOUT, (client ? "Client" : "Server") + " timeout");
			super.policy = policy;
			this.connectTimeout = policy.connectTimeout;
			this.socketTimeout = policy.socketTimeout;
			this.timeout = policy.totalTimeout;
			this.client = client;
		}

		/**
		 * Constructs a client timeout with the given policy and attempt count.
		 *
		 * @param policy the command policy; used to populate timeout fields
		 * @param iteration the attempt count
		 */
		public Timeout(Policy policy, int iteration) {
			super(ResultCode.TIMEOUT, "Client timeout");
			super.policy = policy;
			super.iteration = iteration;
			this.connectTimeout = policy.connectTimeout;
			this.socketTimeout = policy.socketTimeout;
			this.timeout = policy.totalTimeout;
			this.client = true;
		}

		/**
		 * Constructs a client timeout with the node and timeout values.
		 *
		 * @param node the node that was used
		 * @param connectTimeout connect timeout in milliseconds
		 * @param socketTimeout socket idle timeout in milliseconds
		 * @param totalTimeout total timeout in milliseconds
		 */
		public Timeout(Node node, int connectTimeout, int socketTimeout, int totalTimeout) {
			super(ResultCode.TIMEOUT, "Client timeout");
			super.node = node;
			super.iteration = 1;

			Policy p = new Policy();
			p.connectTimeout = connectTimeout;
			p.socketTimeout = socketTimeout;
			p.totalTimeout = totalTimeout;
			p.maxRetries = 0;
			super.policy = p;

			this.connectTimeout = connectTimeout;
			this.socketTimeout = socketTimeout;
			this.timeout = totalTimeout;
			this.client = true;
		}
	}

	/**
	 * Thrown when a Java serialization error occurs while serializing or deserializing data.
	 *
	 * @see ResultCode#SERIALIZE_ERROR
	 */
	public static final class Serialize extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Constructs a serialize exception with the given cause.
		 *
		 * @param e the cause; may be {@code null}
		 */
		public Serialize(Throwable e) {
			super(ResultCode.SERIALIZE_ERROR, "Serialize error", e);
		}

		/**
		 * Constructs a serialize exception with the given message.
		 *
		 * @param message the detail message; may be {@code null}
		 */
		public Serialize(String message) {
			super(ResultCode.SERIALIZE_ERROR, message);
		}
	}

	/**
	 * Thrown when the client cannot parse data returned from the server.
	 *
	 * @see ResultCode#PARSE_ERROR
	 */
	public static final class Parse extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Constructs a parse exception with the given message.
		 *
		 * @param message the detail message; may be {@code null}
		 */
		public Parse(String message) {
			super(ResultCode.PARSE_ERROR, message);
		}
	}

	/**
	 * Thrown when the client cannot connect to the server (e.g. host unreachable, connection refused).
	 *
	 * @see ResultCode#SERVER_NOT_AVAILABLE
	 */
	public static final class Connection extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Constructs a connection exception with the given message.
		 *
		 * @param message the detail message; may be {@code null}
		 */
		public Connection(String message) {
			super(ResultCode.SERVER_NOT_AVAILABLE, message);
		}

		/**
		 * Constructs a connection exception with the given cause.
		 *
		 * @param e the cause; may be {@code null}
		 */
		public Connection(Throwable e) {
			super(ResultCode.SERVER_NOT_AVAILABLE, "Connection failed", e);
		}

		/**
		 * Constructs a connection exception with the given message and cause.
		 *
		 * @param message the detail message; may be {@code null}
		 * @param e the cause; may be {@code null}
		 */
		public Connection(String message, Throwable e) {
			super(ResultCode.SERVER_NOT_AVAILABLE, message, e);
		}

		/**
		 * Constructs a connection exception with the given result code and message.
		 *
		 * @param resultCode the result code
		 * @param message the detail message; may be {@code null}
		 */
		public Connection(int resultCode, String message) {
			super(resultCode, message);
		}
	}

	/**
	 * Thrown when the selected node is not active, or when the namespace is removed after the client has connected.
	 *
	 * @see ResultCode#INVALID_NODE_ERROR
	 */
	public static final class InvalidNode extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Constructs an invalid-node exception for the given cluster size and partition.
		 *
		 * @param clusterSize current cluster size (0 implies "cluster is empty")
		 * @param partition the partition that had no node
		 */
		public InvalidNode(int clusterSize, Partition partition) {
			super(ResultCode.INVALID_NODE_ERROR,
				(clusterSize == 0) ? "Cluster is empty" : "Node not found for partition " + partition);
		}

		/**
		 * Constructs an invalid-node exception for the given partition ID.
		 *
		 * @param partitionId the partition that had no node
		 */
		public InvalidNode(int partitionId) {
			super(ResultCode.INVALID_NODE_ERROR, "Node not found for partition " + partitionId);
		}

		/**
		 * Constructs an invalid-node exception with the given message.
		 *
		 * @param message the detail message; may be {@code null}
		 */
		public InvalidNode(String message) {
			super(ResultCode.INVALID_NODE_ERROR, message);
		}
	}

	/**
	 * Thrown when the namespace is invalid or not found in the partition map.
	 *
	 * @see ResultCode#INVALID_NAMESPACE
	 */
	public static final class InvalidNamespace extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Constructs an invalid-namespace exception for the given namespace and map size.
		 *
		 * @param ns the namespace name
		 * @param mapSize partition map size (0 implies "partition map empty")
		 */
		public InvalidNamespace(String ns, int mapSize) {
			super(ResultCode.INVALID_NAMESPACE,
				(mapSize == 0) ? "Partition map empty" : "Namespace not found in partition map: " + ns);
		}
	}

	/**
	 * Thrown when a batch exists operation fails; partial results are available in {@link #exists}.
	 *
	 * @see ResultCode#BATCH_FAILED
	 */
	public static final class BatchExists extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Existence result for each key in the batch; one entry per key in request order.
		 */
		public final boolean[] exists;

		/**
		 * Constructs a batch-exists exception with the partial results and cause.
		 *
		 * @param exists existence result per key; may be {@code null}
		 * @param e the cause; may be {@code null}
		 */
		public BatchExists(boolean[] exists, Throwable e) {
			super(ResultCode.BATCH_FAILED, "Batch failed", e);
			this.exists = exists;
		}
	}

	/**
	 * Thrown when a batch read operation fails; partial results are available in {@link #records}.
	 *
	 * <p>Each element corresponds to one key: non-null for succeeded requests, {@code null} for failed ones.
	 *
	 * @see ResultCode#BATCH_FAILED
	 */
	public static final class BatchRecords extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Record result for each key in the batch; {@code null} entries indicate failed keys.
		 */
		public final Record[] records;

		/**
		 * Constructs a batch-records exception with the partial results and cause.
		 *
		 * @param records record per key; {@code null} entries for failures; may be {@code null}
		 * @param e the cause; may be {@code null}
		 */
		public BatchRecords(Record[] records, Throwable e) {
			super(ResultCode.BATCH_FAILED, "Batch failed", e);
			this.records = records;
		}
	}

	/**
	 * Thrown when a batch write operation fails; partial results are available in {@link #records}.
	 *
	 * <p>Each {@link BatchRecord} contains the result code and optional record for that key.
	 *
	 * @see ResultCode#BATCH_FAILED
	 */
	public static final class BatchRecordArray extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Result for each key in the batch; each element has result code and optional record.
		 */
		public final BatchRecord[] records;

		/**
		 * Constructs a batch-record-array exception with the partial results and cause.
		 *
		 * @param records result per key; may be {@code null}
		 * @param e the cause; may be {@code null}
		 */
		public BatchRecordArray(BatchRecord[] records, Throwable e) {
			super(ResultCode.BATCH_FAILED, "Batch failed", e);
			this.records = records;
		}

		/**
		 * Constructs a batch-record-array exception with the given message and cause.
		 *
		 * @param records result per key; may be {@code null}
		 * @param message the detail message; may be {@code null}
		 * @param e the cause; may be {@code null}
		 */
		public BatchRecordArray(BatchRecord[] records, String message, Throwable e) {
			super(ResultCode.BATCH_FAILED, message, e);
			this.records = records;
		}
	}

	/**
	 * Thrown when a scan was terminated before completion (e.g. by the user or due to an error).
	 *
	 * @see ResultCode#SCAN_TERMINATED
	 */
	public static final class ScanTerminated extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/** Constructs a scan-terminated exception with no message. */
		public ScanTerminated() {
			super(ResultCode.SCAN_TERMINATED);
		}

		/**
		 * Constructs a scan-terminated exception with the given cause.
		 *
		 * @param e the cause; may be {@code null}
		 */
		public ScanTerminated(Throwable e) {
			super(ResultCode.SCAN_TERMINATED, "Scan terminated", e);
		}
	}

	/**
	 * Thrown when a query was terminated before completion (e.g. by the user or due to an error).
	 *
	 * @see ResultCode#QUERY_TERMINATED
	 */
	public static final class QueryTerminated extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/** Constructs a query-terminated exception with no message. */
		public QueryTerminated() {
			super(ResultCode.QUERY_TERMINATED);
		}

		/**
		 * Constructs a query-terminated exception with the given cause.
		 *
		 * @param e the cause; may be {@code null}
		 */
		public QueryTerminated(Throwable e) {
			super(ResultCode.QUERY_TERMINATED, "Query terminated", e);
		}
	}

	/**
	 * Thrown when an async command was rejected because the async delay queue is full.
	 *
	 * @see ResultCode#ASYNC_QUEUE_FULL
	 */
	public static final class AsyncQueueFull extends Backoff {
		private static final long serialVersionUID = 1L;

		/** Constructs an async-queue-full exception. */
		public AsyncQueueFull() {
			super(ResultCode.ASYNC_QUEUE_FULL);
		}
	}

	/**
	 * Thrown when a node is in backoff mode due to an excessive error rate (e.g. circuit breaker open).
	 *
	 * @see AerospikeException.AsyncQueueFull
	 */
	public static class Backoff extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Constructs a backoff exception with the given result code.
		 *
		 * @param resultCode the result code (e.g. {@link ResultCode#ASYNC_QUEUE_FULL})
		 */
		public Backoff(int resultCode) {
			super(resultCode);
		}
	}

	/**
	 * Thrown when a transaction commit fails; details are in {@link #error}, {@link #verifyRecords}, and {@link #rollRecords}.
	 *
	 * @see ResultCode#TXN_FAILED
	 */
	public static final class Commit extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Error status of the attempted commit.
		 */
		public final CommitError error;

		/**
		 * Verify result for each read key in the transaction; {@code null} if failure occurred before verify.
		 */
		public final BatchRecord[] verifyRecords;

		/**
		 * Roll forward/backward result for each write key; {@code null} if failure occurred before roll.
		 */
		public final BatchRecord[] rollRecords;

		/**
		 * Constructs a commit exception with the given error and partial results.
		 *
		 * @param error the commit error status
		 * @param verifyRecords verify result per read key; may be {@code null}
		 * @param rollRecords roll result per write key; may be {@code null}
		 */
		public Commit(CommitError error, BatchRecord[] verifyRecords, BatchRecord[] rollRecords) {
			super(ResultCode.TXN_FAILED, error.str);
			this.error = error;
			this.verifyRecords = verifyRecords;
			this.rollRecords = rollRecords;
		}

		/**
		 * Constructs a commit exception with the given error, partial results, and cause.
		 *
		 * @param error the commit error status
		 * @param verifyRecords verify result per read key; may be {@code null}
		 * @param rollRecords roll result per write key; may be {@code null}
		 * @param cause the cause; may be {@code null}
		 */
		public Commit(CommitError error, BatchRecord[] verifyRecords, BatchRecord[] rollRecords, Throwable cause) {
			super(ResultCode.TXN_FAILED, error.str, cause);
			this.error = error;
			this.verifyRecords = verifyRecords;
			this.rollRecords = rollRecords;
		}

		@Override
		public String getMessage() {
			String msg = super.getMessage();
			StringBuilder sb = new StringBuilder(1024);
			recordsToString(sb, "verify errors:", verifyRecords);
			recordsToString(sb, "roll errors:", rollRecords);
			return msg + sb.toString();
		}
	}

	private static void recordsToString(StringBuilder sb, String title, BatchRecord[] records) {
		if (records == null) {
			return;
		}

		int count = 0;

		for (BatchRecord br : records) {
			// Only show results with an error response.
			if (!(br.resultCode == ResultCode.OK || br.resultCode == ResultCode.NO_RESPONSE)) {
				// Only show first 3 errors.
				if (count >= 3) {
					sb.append(System.lineSeparator());
					sb.append("...");
					break;
				}

				if (count == 0) {
					sb.append(System.lineSeparator());
					sb.append(title);
				}

				sb.append(System.lineSeparator());
				sb.append(br.key);
				sb.append(',');
				sb.append(br.resultCode);
				sb.append(',');
				sb.append(br.inDoubt);
				count++;
			}
		}
	}
}
