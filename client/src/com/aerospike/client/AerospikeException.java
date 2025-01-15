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
 * Aerospike exceptions that can be thrown from the client.
 */
public class AerospikeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	protected transient Node node;
	protected transient Policy policy;
	protected List<AerospikeException> subExceptions;
	protected int resultCode = ResultCode.CLIENT_ERROR;
	protected int iteration = -1;
	protected boolean inDoubt;

	public AerospikeException(int resultCode, String message) {
		super(message);
		this.resultCode = resultCode;
	}

	public AerospikeException(int resultCode, Throwable e) {
		super(e);
		this.resultCode = resultCode;
	}

	public AerospikeException(int resultCode) {
		super();
		this.resultCode = resultCode;
	}

	public AerospikeException(int resultCode, boolean inDoubt) {
		super();
		this.resultCode = resultCode;
		this.inDoubt = inDoubt;
	}

	public AerospikeException(int resultCode, String message, Throwable e) {
		super(message, e);
		this.resultCode = resultCode;
	}

	public AerospikeException(String message, Throwable e) {
		super(message, e);
	}

	public AerospikeException(String message) {
		super(message);
	}

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
	 * Return base message without extra metadata.
	 */
	public String getBaseMessage() {
		String message = super.getMessage();
		return (message != null)? message : ResultCode.getResultString(resultCode);
	}

	/**
	 * Should connection be put back into pool.
	 */
	public final boolean keepConnection() {
		return ResultCode.keepConnection(resultCode);
	}

	/**
	 * Get last node used.
	 */
	public final Node getNode() {
		return node;
	}

	/**
	 * Set last node used.
	 */
	public final void setNode(Node node) {
		this.node = node;
	}

	/**
	 * Get command policy.  Will be null for non-command exceptions.
	 */
	public final Policy getPolicy() {
		return policy;
	}

	/**
	 * Set command policy.
	 */
	public final void setPolicy(Policy policy) {
		this.policy = policy;
	}

	/**
	 * Get sub exceptions.  Will be null if a retry did not occur.
	 */
	public final List<AerospikeException> getSubExceptions() {
		return subExceptions;
	}

	/**
	 * Set sub exceptions.
	 */
	public final void setSubExceptions(List<AerospikeException> subExceptions) {
		this.subExceptions = subExceptions;
	}

	/**
	 * Get integer result code.
	 */
	public final int getResultCode() {
		return resultCode;
	}

	/**
	 * Get number of attempts before failing.
	 */
	public final int getIteration() {
		return iteration;
	}

	/**
	 * Set number of attempts before failing.
	 */
	public final void setIteration(int iteration) {
		this.iteration = iteration;
	}

	/**
	 * Is it possible that write command may have completed.
	 */
	public final boolean getInDoubt() {
		return inDoubt;
	}

	/**
	 * Set whether it is possible that the write command may have completed
	 * even though this exception was generated.  This may be the case when a
	 * client error occurs (like timeout) after the command was sent to the server.
	 */
	public final void setInDoubt(boolean isWrite, int commandSentCounter) {
		if (isWrite && (commandSentCounter > 1 || (commandSentCounter == 1 && (resultCode == ResultCode.TIMEOUT || resultCode <= 0)))) {
			this.inDoubt = true;
		}
	}

	/**
	 * Sets the inDoubt value to inDoubt.
	 */
	public void setInDoubt(boolean inDoubt) {
		this.inDoubt = inDoubt;
	}

	/**
	 * Exception thrown when database request expires before completing.
	 */
	public static final class Timeout extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Socket initial connect timeout in milliseconds.
		 * This field is redundant and is only left here for backwards compatibility.
		 */
		public int connectTimeout;

		/**
		 * Socket idle timeout in milliseconds.
		 * This field is redundant and is only left here for backwards compatibility.
		 */
		public int socketTimeout;

		/**
		 * Total timeout in milliseconds.
		 * This field is redundant and is only left here for backwards compatibility.
		 */
		public int timeout;

		/**
		 * If true, client initiated timeout.  If false, server initiated timeout.
		 */
		public boolean client;

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

		public Timeout(Policy policy, boolean client) {
			// Other base exception fields are set after this constructor.
			super(ResultCode.TIMEOUT, (client ? "Client" : "Server") + " timeout");
			super.policy = policy;
			this.connectTimeout = policy.connectTimeout;
			this.socketTimeout = policy.socketTimeout;
			this.timeout = policy.totalTimeout;
			this.client = client;
		}

		public Timeout(Policy policy, int iteration) {
			super(ResultCode.TIMEOUT, "Client timeout");
			super.policy = policy;
			super.iteration = iteration;
			this.connectTimeout = policy.connectTimeout;
			this.socketTimeout = policy.socketTimeout;
			this.timeout = policy.totalTimeout;
			this.client = true;
		}

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
	 * Exception thrown when a Java serialization error occurs.
	 */
	public static final class Serialize extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Serialize(Throwable e) {
			super(ResultCode.SERIALIZE_ERROR, "Serialize error", e);
		}

		public Serialize(String message) {
			super(ResultCode.SERIALIZE_ERROR, message);
		}
	}

	/**
	 * Exception thrown when client can't parse data returned from server.
	 */
	public static final class Parse extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Parse(String message) {
			super(ResultCode.PARSE_ERROR, message);
		}
	}

	/**
	 * Exception thrown when client can't connect to the server.
	 */
	public static final class Connection extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Connection(String message) {
			super(ResultCode.SERVER_NOT_AVAILABLE, message);
		}

		public Connection(Throwable e) {
			super(ResultCode.SERVER_NOT_AVAILABLE, "Connection failed", e);
		}

		public Connection(String message, Throwable e) {
			super(ResultCode.SERVER_NOT_AVAILABLE, message, e);
		}

		public Connection(int resultCode, String message) {
			super(resultCode, message);
		}
	}

	/**
	 * Exception thrown when chosen node is not active.
	 */
	public static final class InvalidNode extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public InvalidNode(int clusterSize, Partition partition) {
			super(ResultCode.INVALID_NODE_ERROR,
				(clusterSize == 0) ? "Cluster is empty" : "Node not found for partition " + partition);
		}

		public InvalidNode(int partitionId) {
			super(ResultCode.INVALID_NODE_ERROR, "Node not found for partition " + partitionId);
		}

		public InvalidNode(String message) {
			super(ResultCode.INVALID_NODE_ERROR, message);
		}
	}

	/**
	 * Exception thrown when namespace is invalid.
	 */
	public static final class InvalidNamespace extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public InvalidNamespace(String ns, int mapSize) {
			super(ResultCode.INVALID_NAMESPACE,
				(mapSize == 0) ? "Partition map empty" : "Namespace not found in partition map: " + ns);
		}
	}

	/**
	 * Exception thrown when a batch exists method fails.
	 */
	public static final class BatchExists extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public final boolean[] exists;

		public BatchExists(boolean[] exists, Throwable e) {
			super(ResultCode.BATCH_FAILED, "Batch failed", e);
			this.exists = exists;
		}
	}

	/**
	 * Exception thrown when a batch read method fails.
	 * The records fields contains responses for key requests that succeeded and null
	 * records for key requests that failed.
	 */
	public static final class BatchRecords extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public final Record[] records;

		public BatchRecords(Record[] records, Throwable e) {
			super(ResultCode.BATCH_FAILED, "Batch failed", e);
			this.records = records;
		}
	}

	/**
	 * Exception thrown when a batch write method fails.
	 * The records fields contains responses for key requests that succeeded
	 * and resultCodes for key requests that failed.
	 */
	public static final class BatchRecordArray extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public final BatchRecord[] records;

		public BatchRecordArray(BatchRecord[] records, Throwable e) {
			super(ResultCode.BATCH_FAILED, "Batch failed", e);
			this.records = records;
		}

		public BatchRecordArray(BatchRecord[] records, String message, Throwable e) {
			super(ResultCode.BATCH_FAILED, message, e);
			this.records = records;
		}
	}

	/**
	 * Exception thrown when scan was terminated prematurely.
	 */
	public static final class ScanTerminated extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public ScanTerminated() {
			super(ResultCode.SCAN_TERMINATED);
		}

		public ScanTerminated(Throwable e) {
			super(ResultCode.SCAN_TERMINATED, "Scan terminated", e);
		}
	}

	/**
	 * Exception thrown when query was terminated prematurely.
	 */
	public static final class QueryTerminated extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public QueryTerminated() {
			super(ResultCode.QUERY_TERMINATED);
		}

		public QueryTerminated(Throwable e) {
			super(ResultCode.QUERY_TERMINATED, "Query terminated", e);
		}
	}

	/**
	 * Exception thrown when async command was rejected because the
	 * async delay queue is full.
	 */
	public static final class AsyncQueueFull extends Backoff {
		private static final long serialVersionUID = 1L;

		public AsyncQueueFull() {
			super(ResultCode.ASYNC_QUEUE_FULL);
		}
	}

	/**
	 * Exception thrown when node is in backoff mode due to excessive
	 * number of errors.
	 */
	public static class Backoff extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Backoff(int resultCode) {
			super(resultCode);
		}
	}

	/**
	 * Exception thrown when a transaction commit fails.
	 */
	public static final class Commit extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Error status of the attempted commit.
		 */
		public final CommitError error;

		/**
		 * Verify result for each read key in the transaction. May be null if failure occurred
		 * before verify.
		 */
		public final BatchRecord[] verifyRecords;

		/**
		 * Roll forward/backward result for each write key in the transaction. May be null if
		 * failure occurred before roll forward/backward.
		 */
		public final BatchRecord[] rollRecords;

		public Commit(CommitError error, BatchRecord[] verifyRecords, BatchRecord[] rollRecords) {
			super(ResultCode.TXN_FAILED, error.str);
			this.error = error;
			this.verifyRecords = verifyRecords;
			this.rollRecords = rollRecords;
		}

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
