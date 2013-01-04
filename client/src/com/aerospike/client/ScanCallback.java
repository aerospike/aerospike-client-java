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

import java.util.Map;

/**
 * An object implementing this interface is passed in <code>scan()</code>
 * calls, so the caller can be notified with scan results.
 */
public interface ScanCallback {
	/**
	 * This method will be called for each record returned from a scan.
	 * 
	 * @param namespace			namespace
	 * @param set				set name
	 * @param digest			unique ID generated from key and set name
	 * @param bins				bin name/value pairs as <code>Map</code>
	 * @param generation		how many times the record has been modified
	 * @param expirationDate	date this record will expire, in seconds
	 * 							from Jan 01 2010 00:00:00 GMT
	 */
	public void scanCallback(
		String namespace,
		String set,
		byte[] digest,
		Map<String, Object> bins,
		int generation,
		int expirationDate
		);
}
