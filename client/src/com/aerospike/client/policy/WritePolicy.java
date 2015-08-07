/*
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
	 * Desired consistency guarantee when committing a transaction on the server. The default 
	 * (COMMIT_ALL) indicates that the server should wait for master and all replica commits to 
	 * be successful before returning success to the client.
	 */
	public CommitLevel commitLevel = CommitLevel.COMMIT_ALL;

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
	 * <li>-1: Never expire. Supported by Aerospike 2 server versions >= 2.7.2 and Aerospike 3 server
	 * versions >= 3.1.4.</li>
	 * <li>0: Default to namespace configuration variable "default-ttl" on the server.</li>
	 * <li>> 0: Actual expiration in seconds.<br></li>
	 * </ul>
	 */
	public int expiration;
	
	/**
	 * Copy write policy from another write policy.
	 */
	public WritePolicy(WritePolicy other) {
		super(other);
		this.recordExistsAction = other.recordExistsAction;
		this.generationPolicy = other.generationPolicy;
		this.commitLevel = other.commitLevel;
		this.generation = other.generation;
		this.expiration = other.expiration;
	}

	/**
	 * Copy write policy from another policy.
	 */
	public WritePolicy(Policy other) {
		super(other);
	}
	
	/**
	 * Default constructor.
	 */
	public WritePolicy() {
	}
}
