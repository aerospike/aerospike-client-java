/*
 * Aerospike Client - Java Library
 *
 * Copyright 2014 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.policy;

/**
 * How to handle record writes based on record generation.
 */
public enum GenerationPolicy {
	/**
	 * Do not use record generation to restrict writes.
	 */
	NONE,
	
	/**
	 * Update/delete record if expected generation is equal to server generation. Otherwise, fail. 
	 */
	EXPECT_GEN_EQUAL,

	/**
	 * Update/delete record if expected generation greater than the server generation. Otherwise, fail.
	 * This is useful for restore after backup. 
	 */
	EXPECT_GEN_GT,
	
	/**
	 * Create duplicate record if expected generation is not equal to server generation.
	 * Duplicates are only created when the server configuration option "allow-versions" 
	 * is true (default is false).
	 */
	DUPLICATE
}
