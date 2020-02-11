/*
 * Copyright 2012-2020 Aerospike, Inc.
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
 * Read policy for SC (strong consistency) namespaces.
 * <p>
 * Determines SC read consistency options.
 */
public enum ReadModeSC {
	/**
	 * Ensures this client will only see an increasing sequence of record versions.
	 * Server only reads from master.  This is the default.
	 */
	SESSION,

	/**
	 * Ensures ALL clients will only see an increasing sequence of record versions.
	 * Server only reads from master.
	 */
	LINEARIZE,

	/**
	 * Server may read from master or any full (non-migrating) replica.
	 * Increasing sequence of record versions is not guaranteed.
	 */
	ALLOW_REPLICA,

	/**
	 * Server may read from master or any full (non-migrating) replica or from unavailable
	 * partitions.  Increasing sequence of record versions is not guaranteed.
	 */
	ALLOW_UNAVAILABLE
}
