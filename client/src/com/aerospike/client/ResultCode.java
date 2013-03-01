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
 * Database operation error codes.
 */
public final class ResultCode {
	/**
	 * Query was terminated by user.
	 */
	public static final int QUERY_TERMINATED = -5;

	/**
	 * Scan was terminated by user.
	 */
	public static final int SCAN_TERMINATED = -4;
	
	/**
	 * Chosen node is not currently active.
	 */
	public static final int INVALID_NODE_ERROR = -3;

	/**
	 * Client serialization error.
	 */
	public static final int PARSE_ERROR = -2;

	/**
	 * Client serialization error.
	 */
	public static final int SERIALIZE_ERROR = -1;

	/**
	 * Operation was successful.
	 */
	public static final int OK = 0;
		
	/**
	 * Unknown server failure.
	 */
	public static final int SERVER_ERROR = 1;
		
	/**
	 * On retrieving, touching or replacing a record that doesn't exist.
	 */	
	public static final int KEY_NOT_FOUND_ERROR = 2;
		
	/**
	 * On modifying a record with unexpected generation.
	 */
	public static final int GENERATION_ERROR = 3;

	/**
	 * Bad parameter(s) were passed in database operation call.
	 */
	public static final int PARAMETER_ERROR = 4;

	/**
	 * On create-only (write unique) operations on a record that already
	 * exists.
	 */
	public static final int KEY_EXISTS_ERROR = 5;

	/**
	 * On create-only (write unique) operations on a bin that already
	 * exists.
	 */
	public static final int BIN_EXISTS_ERROR = 6;

	/**
	 * Expected cluster ID was not received.
	 */
	public static final int CLUSTER_KEY_MISMATCH = 7;

	/**
	 * Server has run out of memory.
	 */
	public static final int SERVER_MEM_ERROR = 8;

	/**
	 * Client or server has timed out.
	 */
	public static final int TIMEOUT = 9;

	/**
	 * XDS product is not available.
	 */
	public static final int NO_XDS = 10;

	/**
	 * Server is not accepting requests.
	 */
	public static final int SERVER_NOT_AVAILABLE = 11;

	/**
	 * Operation is not supported with configured bin type (single-bin or
	 * multi-bin).
	 */
	public static final int BIN_TYPE_ERROR = 12;

	/**
	 * Record size exceeds limit.
	 */
	public static final int RECORD_TOO_BIG = 13;

	/**
	 * Too many concurrent operations on the same record.
	 */
	public static final int KEY_BUSY = 14;
	
	
	/**
	 * Return result code as a string.
	 */
	public static String getResultString(int resultCode) {
		switch (resultCode) {
		case INVALID_NODE_ERROR:
			return "Invalid node";

		case PARSE_ERROR:
			return "Parse error";

		case SERIALIZE_ERROR:
			return "Serialize error";

		case OK:
			return "ok";
			
		case SERVER_ERROR:		
			return "Server error";
			
		case KEY_NOT_FOUND_ERROR:		
			return "Key not found";
		
		case GENERATION_ERROR:
			return "Generation error";
			
		case PARAMETER_ERROR:
			return "Parameter error";
		
		case KEY_EXISTS_ERROR:
			return "Key already exists";
		
		case BIN_EXISTS_ERROR:
			return "Bin already exists";
		
		case CLUSTER_KEY_MISMATCH:
			return "Cluster key mismatch";
		
		case SERVER_MEM_ERROR:
			return "Server memory error";
		
		case TIMEOUT:
			return "Timeout";
		
		case NO_XDS:
			return "XDS not available";
		
		case SERVER_NOT_AVAILABLE:
			return "Server not available";
		
		case BIN_TYPE_ERROR:
			return "Bin type error";
		
		case RECORD_TOO_BIG:
			return "Record too big";
		
		case KEY_BUSY:
			return "Hot key";
			
		default:
			return "";
		}
	}
}
