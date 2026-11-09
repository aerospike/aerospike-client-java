/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.client.discovery;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.aerospike.client.Host;

/**
 * Read-only context passed to {@link SeedCandidateProvider#refreshSeedCandidates(SeedCandidateContext)}
 * on each call.
 */
public final class SeedCandidateContext {
	/**
	 * Expected cluster name from {@link com.aerospike.client.policy.ClientPolicy#clusterName}.
	 * May be null.
	 */
	public final String clusterName;

	/**
	 * Static seed hosts passed to the client constructor. Unmodifiable.
	 */
	public final List<Host> configuredSeeds;

	/**
	 * Cluster tend count when the provider is called: 0 at client initialization, then the number
	 * of completed tend iterations. Informational only.
	 */
	public final long tendIteration;

	/**
	 * Initialize seed candidate context.
	 */
	public SeedCandidateContext(String clusterName, Host[] configuredSeeds, long tendIteration) {
		this.clusterName = clusterName;
		this.configuredSeeds = Collections.unmodifiableList(Arrays.asList(configuredSeeds.clone()));
		this.tendIteration = tendIteration;
	}

	@Override
	public String toString() {
		return clusterName + ' ' + configuredSeeds + ' ' + tendIteration;
	}
}
