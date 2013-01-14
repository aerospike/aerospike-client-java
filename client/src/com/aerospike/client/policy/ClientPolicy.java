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

import java.util.concurrent.ExecutorService;

import com.aerospike.client.sql.Statement;

/**
 * Container object for client policy attributes.
 */
public final class ClientPolicy {
	/**
	 * Executer thread service. The service is used to manage asynchronous tasks returned by
	 * {@link com.aerospike.client.AerospikeClient#queryTask(QueryPolicy, Statement, com.aerospike.client.sql.QueryTask.Callback) queryTask()}
	 * and 
	 * {@link com.aerospike.client.AerospikeClient#executeTask(WritePolicy, Statement, com.aerospike.client.sql.ExecuteTask.Callback) executeTask()}
	 * methods.
	 */
	public ExecutorService executerService;
	
	/**
	 * Initial host connection timeout in milliseconds.  The timeout when opening a connection 
	 * to the server host for the first time.
	 */
	public int timeout = 500;

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
