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
 * Where the client runs periodic {@link SeedCandidateProvider#refreshSeedCandidates()} calls.
 * Providers never start threads; the client schedules every refresh. No periodic refresh is
 * scheduled in either mode when {@link SeedCandidateProvider#needsPeriodicRefresh()} is false.
 */
public enum DiscoveryExecutionMode {
	/**
	 * Refresh on a dedicated daemon thread owned by the client instance. The thread is started only
	 * when the provider needs periodic refresh and is stopped when the client is closed. The cluster
	 * tend thread never waits on it. A refresh that exceeds
	 * {@link com.aerospike.client.policy.ClientPolicy#discoveryRefreshTimeout} is not interrupted;
	 * its result is discarded and the last good seed list is kept. This is the default.
	 */
	THREAD,

	/**
	 * Refresh inline on the cluster tend thread. Requires a provider whose
	 * {@link SeedCandidateProvider#supportsDeadline()} returns true. Each call is timed against
	 * {@link com.aerospike.client.policy.ClientPolicy#discoveryTendRefreshDeadline}. The deadline is
	 * measured, not enforced: an overrunning call blocks tend for its full duration, its result is
	 * discarded and the last good seed list is kept. After three consecutive overruns, the provider
	 * is no longer called from tend for the life of the client instance and the seed list reverts to
	 * the one established at client initialization.
	 */
	TEND
}
