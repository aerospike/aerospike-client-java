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
 * Database operation definition.  The class is used in client's operate() method. 
 */
public final class Operation {
	// Operations commented out are not supported by this client.
	public static final int READ = 1;
	public static final int WRITE = 2;
	//public static final int WRITE_UNIQUE = 3;
	//public static final int WRITE_NOW = 4;
	public static final int ADD = 5;
	//public static final int APPEND_SEGMENT = 6;
	//public static final int APPEND_SEGMENT_EXT = 7;
	//public static final int APPEND_SEGMENT_QUERY = 8;
	public static final int APPEND = 9;
	public static final int PREPEND = 10;
	public static final int TOUCH = 11;

	/**
	 * Create read bin database operation.
	 */
	public static Operation get(String binName) {
		return new Operation(READ, binName);
	}

	/**
	 * Create read all record bins database operation.
	 */
	public static Operation get() {
		return new Operation(READ);
	}

	/**
	 * Create read record header database operation.
	 */
	public static Operation getHeader() {
		// Overload bin value to indicator only record header should be read.
		return new Operation(READ, null, 1);
	}

	/**
	 * Create set database operation.
	 */
	public static Operation put(String binName, Object binValue) {
		return new Operation(WRITE, binName, binValue);
	}

	/**
	 * Create set database operation.
	 */
	public static Operation put(Bin bin) {
		return new Operation(WRITE, bin.name, bin.value);
	}

	/**
	 * Create string append database operation.
	 */
	public static Operation append(String binName, Object binValue) {
		return new Operation(APPEND, binName, binValue);
	}

	/**
	 * Create string append database operation.
	 */
	public static Operation append(Bin bin) {
		return new Operation(APPEND, bin.name, bin.value);
	}

	/**
	 * Create string prepend database operation.
	 */
	public static Operation prepend(String binName, Object binValue) {
		return new Operation(PREPEND, binName, binValue);
	}
	
	/**
	 * Create string prepend database operation.
	 */
	public static Operation prepend(Bin bin) {
		return new Operation(PREPEND, bin.name, bin.value);
	}

	/**
	 * Create integer add database operation.
	 */
	public static Operation add(String binName, Object binValue) {
		return new Operation(ADD, binName, binValue);
	}
	
	/**
	 * Create integer add database operation.
	 */
	public static Operation add(Bin bin) {
		return new Operation(ADD, bin.name, bin.value);
	}

	/**
	 * Create touch database operation.
	 */
	public static Operation touch() {
		return new Operation(TOUCH);
	}

	/**
	 * Type of operation.
	 */
	public final int type;
	
	/**
	 * Optional bin name used in operation.
	 */
	public final String binName;
	
	/**
	 * Optional bin value used in operation.
	 */
	public final Object binValue;
	 
	private Operation(int type, String binName, Object binValue) {
		this.type = type;
		this.binName = binName;
		this.binValue = binValue;
	}
	
	private Operation(int type, String binName) {
		this.type = type;
		this.binName = binName;
		this.binValue = null;
	}
	
	private Operation(int type) {
		this.type = type;
		this.binName = null;
		this.binValue = null;
	}
}
