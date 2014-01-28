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
 * Container object for policy attributes used in write operations.
 * This object is passed into methods where database writes can occur.
 */
public final class WritePolicy extends Policy {
	/**
	 * Qualify how to handle writes where the record already exists.
	 */
	public RecordExistsAction recordExistsAction = RecordExistsAction.UPDATE;

	/**
	 * Qualify how to handle record writes based on record generation. The default (NONE)
	 * indicates that the generation is not used to restrict writes.
	 */
	public GenerationPolicy generationPolicy = GenerationPolicy.NONE;

	/**
	 * Expected generation. Generation is the number of times a record has been modified
	 * (including creation) on the server. If a write operation is creating a record, 
	 * the expected generation would be <code>0</code>.  
	 */
	public int generation;

	/**
	 * Record expiration. Also known as ttl (time to live).
	 * Seconds record will live before being removed by the server.
	 * <p>
	 * Expiration values:
	 * <ul>
	 * <li>-1: Never expire for Aerospike 2 server versions >= 2.7.2 and Aerospike 3 server
	 * versions >= 3.1.4.  For older servers, -1 means a very long (max integer) expiration.</li>
	 * <li>0: Default to namespace configuration variable "default-ttl" on the server.</li>
	 * <li>> 0: Actual expiration in seconds.<br></li>
	 * </ul>
	 */
	public int expiration;	
}
