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
 * How to handle writes when the record already exists.
 */
public enum RecordExistsAction {
	/**
	 * Update record. 
	 */
	UPDATE,

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
	 * Fail if record exists. 
	 */
	FAIL,
			
	/**
	 * Create duplicate record if expected generation is not equal to server generation.
	 */
	DUPLICATE
}
