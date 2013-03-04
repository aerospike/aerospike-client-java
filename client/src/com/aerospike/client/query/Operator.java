/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.query;

/**
 * Query filter operators.
 */
public enum Operator {
	/**
	 * Equals (=)
	 */
	EQUALS,
	
	/**
	 * Less than (<)
	 */
	LT,
	
	/**
	 * Less than or equal (<=)
	 */
	LTE,
	
	/**
	 * Greater than (>)
	 */
	GT,
	
	/**
	 * Greater than or equal (>=)
	 */
	GTE;
}
