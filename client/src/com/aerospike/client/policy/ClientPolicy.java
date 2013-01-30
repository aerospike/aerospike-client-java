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
 * Container object for client policy attributes.
 */
public final class ClientPolicy {
	/**
	 * Initial host connection timeout in milliseconds.  The timeout when opening a connection 
	 * to the server host for the first time.
	 */
	public int timeout = 1000;

	/**
	 * Estimate of threads concurrently using the client instance.  This field is used to size
	 * connection pools.
	 */
	public int maxThreads = 300;
	
	/**
	 * Throw exception if host connection fails during addHost().
	 */
	public boolean failIfNotConnected;
}
