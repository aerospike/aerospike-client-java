/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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

/**
 * Aerospike exceptions that can be thrown from the client.
 */
public class AerospikeException extends Exception {
	private static final long serialVersionUID = 1L;

	private int resultCode;
	
	public AerospikeException(int resultCode, String message) {
		super(message);
		this.resultCode = resultCode;
	}

	public AerospikeException(int resultCode, Exception e) {
		super(e);
		this.resultCode = resultCode;
	}

	public AerospikeException(int resultCode) {
		super();
		this.resultCode = resultCode;
	}
	
	public AerospikeException(String message, Exception e) {
		super(message, e);
	}

	public AerospikeException(String message) {
		super(message);
	}

	public AerospikeException(Throwable e) {
		super(e);
	}
	
	public AerospikeException() {
	}

	@Override
	public String getMessage() {
		StringBuilder sb = new StringBuilder();
		String message = super.getMessage();
		
		if (resultCode != 0) {
			sb.append("Error Code ");
			sb.append(resultCode);
			sb.append(": ");

			if (message != null) {
				sb.append(message);
			}
			else {
				sb.append(ResultCode.getResultString(resultCode));
			}
		}
		else {
			if (message != null) {
				sb.append(message);
			}
			else {
				sb.append(this.getClass().getName());
			}
		}		
		return sb.toString();
	}
	
	/**
	 * Should connection be put back into pool.
	 */
	public final boolean keepConnection() {
		return ResultCode.keepConnection(resultCode);
	}
	
	/**
	 * Get integer result code.
	 */
	public final int getResultCode() {
		return resultCode;
	}
	
	/**
	 * Exception thrown when database request expires before completing.
	 */
	public static final class Timeout extends AerospikeException {
		private static final long serialVersionUID = 1L;
		
		public int timeout;
		public int iterations;
		public int failedNodes;
		public int failedConns;
		
		public Timeout() {
			super(ResultCode.TIMEOUT);
			this.timeout = -1;
		}
		
		public Timeout(int timeout, int iterations, int failedNodes, int failedConns) {
			super(ResultCode.TIMEOUT);
			this.timeout = timeout;
			this.iterations = iterations;
			this.failedNodes = failedNodes;
			this.failedConns = failedConns;
		}
		
		@Override
		public String getMessage() {
			if (timeout == -1) {
				return super.getMessage();
			}
			return "Client timeout: timeout=" + timeout + " iterations=" + iterations + 
				" failedNodes=" + failedNodes + " failedConns=" + failedConns;
		}
	}

	/**
	 * Exception thrown when Java serialization error occurs.
	 */
	public static final class Serialize extends AerospikeException {
		private static final long serialVersionUID = 1L;
		
		public Serialize(Exception e) {
			super(ResultCode.SERIALIZE_ERROR, e);
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
		
		public Connection(Exception e) {
			super(ResultCode.SERVER_NOT_AVAILABLE, e);
		}
	}

	/**
	 * Exception thrown when chosen node is not active.
	 */
	public static final class InvalidNode extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public InvalidNode() {
			super(ResultCode.INVALID_NODE_ERROR);
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
		
		public ScanTerminated(Exception e) {
			super(ResultCode.SCAN_TERMINATED, e);
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
		
		public QueryTerminated(Exception e) {
			super(ResultCode.QUERY_TERMINATED, e);
		}
	}
	
	/**
	 * Exception thrown when asynchronous command was rejected because the 
	 * max concurrent database commands have been exceeded.
	 */
	public static final class CommandRejected extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public CommandRejected() {
			super(ResultCode.COMMAND_REJECTED);
		}
	}
}
