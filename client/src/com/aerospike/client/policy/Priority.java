/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.policy;

/**
 * Priority of operations on database server.
 */
public enum Priority {
	/**
	 * The server defines the priority.
	 */
	DEFAULT,

	/**
	 * Run the database operation in a background thread.
	 */
	LOW,

	/**
	 * Run the database operation at medium priority.
	 */
	MEDIUM,

	/**
	 * Run the database operation at the highest priority.
	 */
	HIGH
}
