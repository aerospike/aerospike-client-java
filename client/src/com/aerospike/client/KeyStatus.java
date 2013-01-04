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
 * KeyStatus is used for batch exists operations.
 */
public final class KeyStatus {
	/**
	 * Unique identifier associated with record.
	 */
	public final Key key;
	
	/**
	 * Whether record exists or not.
	 */
	public final boolean exists;
	
	/**
	 * Initialize.
	 */
	public KeyStatus(Key key, boolean exists) {
		this.key = key;
		this.exists = exists;
	}
}
