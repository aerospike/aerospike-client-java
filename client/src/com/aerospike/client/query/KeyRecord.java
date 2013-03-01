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

import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Container object for key identifier and record data.
 */
public final class KeyRecord {
	/**
	 * Unique identifier for record.
	 */
	public final Key key;
	
	/**
	 * Record header and bin data.
	 */
	public final Record record;
	
	/**
	 * Initialize key and record.
	 */
	public KeyRecord(Key key, Record record) {
		this.key = key;
		this.record = record;	
	}
}
