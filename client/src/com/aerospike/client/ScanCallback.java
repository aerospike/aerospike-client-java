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
 * An object implementing this interface is passed in <code>scan()</code> calls, so the caller can
 * be notified with scan results.
 */
public interface ScanCallback {
	/**
	 * This method will be called for each record returned from a scan. The user may throw a 
	 * {@link com.aerospike.client.AerospikeException.ScanTerminated AerospikeException.ScanTerminated} 
	 * exception if the scan should be aborted.  If any exception is thrown, parallel scan threads
	 * to other nodes will also be terminated and the exception will be propagated back through the
	 * initiating scan call.
	 * 
	 * @param key					unique record identifier
	 * @param record				container for bins and record meta-data
	 * @throws AerospikeException	if error occurs or scan should be terminated.
	 */
	public void scanCallback(Key key, Record record) throws AerospikeException;
}
