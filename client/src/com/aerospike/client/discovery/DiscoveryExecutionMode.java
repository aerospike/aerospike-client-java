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

/**
 * Where periodic {@link SeedCandidateProvider#refreshSeedCandidates(SeedCandidateContext)} calls run.
 * See {@link com.aerospike.client.policy.ClientPolicy#discoveryExecutionMode}.
 */
public enum DiscoveryExecutionMode {
	/**
	 * Refresh on one discovery thread per client instance. The cluster tend thread only reads the
	 * published seed list and never calls the provider, so a slow provider never delays tend.
	 * YAML value: {@code thread}.
	 */
	THREAD,

	/**
	 * Refresh on the cluster tend thread every
	 * {@link com.aerospike.client.policy.ClientPolicy#discoveryRefreshInterval} /
	 * {@link com.aerospike.client.policy.ClientPolicy#tendInterval} tend iterations. Starts no
	 * thread; the provider must support a deadline. YAML value: {@code tend}.
	 */
	TEND
}
