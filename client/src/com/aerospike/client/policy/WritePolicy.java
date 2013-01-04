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
	 * Expected generation. Generation is the number of times a record has been modified
	 * (including creation) on the server. If a write operation is creating a record, 
	 * the expected generation would be <code>0</code>.  
	 */
	public int generation;

	/**
	 * Record expiration. Seconds record will live before being removed by the server.
	 */
	public int expiration;	
}
