/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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

	public AerospikeException(Exception e) {
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
		
		public Timeout() {
			super(ResultCode.TIMEOUT);
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
}
