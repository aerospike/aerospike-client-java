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
	 * Create or update record.
	 * Merge write command bins with existing bins.
	 */
	UPDATE,

	/**
	 * Update record only. Fail if record does not exist.
	 * Merge write command bins with existing bins.
	 */
	UPDATE_ONLY,
	
	/**
	 * Create or update record.
	 * Delete existing bins not referenced by write command bins.
	 * Supported by Aerospike 2 server versions >= 2.7.5 and 
	 * Aerospike 3 server versions >= 3.1.6.
	 */
	REPLACE,
	
	/**
	 * Update record only. Fail if record does not exist.
	 * Delete existing bins not referenced by write command bins.
	 * Supported by Aerospike 2 server versions >= 2.7.5 and 
	 * Aerospike 3 server versions >= 3.1.6.
	 */
	REPLACE_ONLY,

	/**
	 * Create only.  Fail if record exists. 
	 */
	FAIL,
		
	/**
	 * @deprecated Use {@link com.aerospike.client.policy.GenerationPolicy#EXPECT_GEN_EQUAL}
	 * in {@link com.aerospike.client.policy.WritePolicy#generationPolicy} instead. 
	 */
	@Deprecated
	EXPECT_GEN_EQUAL,

	/**
	 * @deprecated Use {@link com.aerospike.client.policy.GenerationPolicy#EXPECT_GEN_GT}
	 * in {@link com.aerospike.client.policy.WritePolicy#generationPolicy} instead. 
	 */
	@Deprecated
	EXPECT_GEN_GT,
	
	/**
	 * @deprecated Use {@link com.aerospike.client.policy.GenerationPolicy#DUPLICATE}
	 * in {@link com.aerospike.client.policy.WritePolicy#generationPolicy} instead. 
	 */
	@Deprecated
	DUPLICATE
}
