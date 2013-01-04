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

import java.util.List;
import java.util.Map;

/**
 * Container object for records.  Records are equivalent to rows.
 */
public final class Record {
	/**
	 * Unique identifier associated with record.
	 */
	public final Key key;
	
	/**
	 * Map of requested name/value bins.
	 */
	public final Map<String,Object> bins;
	
	/**
	 * List of all duplicate records (if any) for a given key.  Duplicates are only created when
	 * the server configuration option "allow-versions" is true (default is false) and client
	 * RecordExistsAction.DUPLICATE policy flag is set and there is a generation error.
	 * Almost always null.
	 */
	public final List<Map<String,Object>> duplicates;
	
	/**
	 * Record modification count.
	 */
	public final int generation;
	
	/**
	 * Date record will expire, in seconds from Jan 01 2010 00:00:00 GMT
	 */
	public final int expiration;

	/**
	 * Initialize record.
	 */
	public Record(
		Key key,
		Map<String,Object> values,
		List<Map<String,Object>> duplicates,
		int generation,
		int expiration
	) {
		this.key = key;
		this.bins = values;
		this.duplicates = duplicates;
		this.generation = generation;
		this.expiration = expiration;
	}
	
	/**
	 * Get bin value given bin name.
	 */
	public Object getValue(String name) {		
		return bins.get(name);
	}	
}
