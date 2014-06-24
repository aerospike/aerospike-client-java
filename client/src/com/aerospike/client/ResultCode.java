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
 * Database operation error codes.  The positive numbers align with the server
 * side file proto.h.
 */
public final class ResultCode {
	/**
	 * Asynchronous max concurrent database commands have been exceeded and therefore rejected.
	 */
	public static final int COMMAND_REJECTED = -6;

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
	 * Client parse error.
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
	 * Scan aborted by server.
	 */
	public static final int SCAN_ABORT = 15;
	
	/**
	 * Unsupported Server Feature (e.g. Scan + UDF)
	 */
	public static final int UNSUPPORTED_FEATURE = 16;
	
	/**
	 * Specified bin name does not exist in record.
	 */
	public static final int BIN_NOT_FOUND = 17;

	/**
	 * Specified bin name does not exist in record.
	 */
	public static final int DEVICE_OVERLOAD = 18;

	/**
	 * Key type mismatch.
	 */
	public static final int KEY_MISMATCH = 19;
	
	/**
	 * A user defined function returned an error code.
	 */
	public static final int UDF_BAD_RESPONSE = 100;
	
	/**
	 * Secondary index already exists.
	 */
	public static final int INDEX_FOUND = 200;
	
	/**
	 * Requested secondary index does not exist.
	 */
	public static final int INDEX_NOTFOUND = 201;
	
	/**
	 * Secondary index memory space exceeded.
	 */
	public static final int INDEX_OOM = 202;
	
	/**
	 * Secondary index not available.
	 */
	public static final int INDEX_NOTREADABLE = 203;
	
	/**
	 * Generic secondary index error.
	 */
	public static final int INDEX_GENERIC = 204;
	
	/**
	 * Index name maximum length exceeded.
	 */
	public static final int INDEX_NAME_MAXLEN = 205;

	/**
	 * Maximum number of indicies exceeded.
	 */
	public static final int INDEX_MAXCOUNT = 206;

	/**
	 * Secondary index query aborted.
	 */
	public static final int QUERY_ABORTED = 210;
	
	/**
	 * Secondary index queue full.
	 */
	public static final int QUERY_QUEUEFULL = 211;
	
	/**
	 * Secondary index query timed out on server.
	 */
	public static final int QUERY_TIMEOUT = 212;
	
	/**
	 * Generic query error.
	 */
	public static final int QUERY_GENERIC = 213;
		
	/**
	 * Should connection be put back into pool.
	 */
	public static boolean keepConnection(int resultCode) {
		switch (resultCode) {
		case 0: // Exception did not originate on server.
		case QUERY_TERMINATED:
		case SCAN_TERMINATED:
		case INVALID_NODE_ERROR:
		case PARSE_ERROR:
		case SERIALIZE_ERROR:
		case SERVER_MEM_ERROR:
		case TIMEOUT:
		case SERVER_NOT_AVAILABLE:
		case SCAN_ABORT:
		case INDEX_OOM:
		case QUERY_ABORTED:
		case QUERY_TIMEOUT:
			return false;
			
		default:
			return true;					
		}
	}

	/**
	 * Return result code as a string.
	 */
	public static String getResultString(int resultCode) {
		switch (resultCode) {
		case COMMAND_REJECTED:
			return "Command rejected";
		
		case QUERY_TERMINATED:
			return "Query terminated";
		
		case SCAN_TERMINATED:
			return "Scan terminated";

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
			
		case SCAN_ABORT:
			return "Scan aborted";
			
		case UNSUPPORTED_FEATURE:
			return "Unsupported Server Feature";
			
		case BIN_NOT_FOUND:
			return "Bin not found";

		case DEVICE_OVERLOAD:
			return "Device overload";

		case KEY_MISMATCH:
			return "Key mismatch";

		case UDF_BAD_RESPONSE:
			return "UDF returned error";
			
		case INDEX_FOUND:
			return "Index already exists";
			
		case INDEX_NOTFOUND:
			return "Index not found";
			
		case INDEX_OOM:
			return "Index out of memory";
			
		case INDEX_NOTREADABLE:
			return "Index not readable";
			
		case INDEX_GENERIC:
			return "Index error";
			
		case INDEX_NAME_MAXLEN:
			return "Index name max length exceeded";
			
		case INDEX_MAXCOUNT:
			return "Index count exceeds max";
		
		case QUERY_ABORTED:
			return "Query aborted";
			
		case QUERY_QUEUEFULL:
			return "Query queue full";
			
		case QUERY_TIMEOUT:
			return "Query timeout";
			
		case QUERY_GENERIC:
			return "Query error";
			
		default:
			return "";
		}
	}
}
