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

import java.util.List;

import com.aerospike.client.Host;

/**
 * Source of seed hosts used to discover the cluster. The client consults the provider when
 * the cluster is initialized and when it must re-seed because no nodes are reachable.
 * <p>
 * When no provider is configured, {@link StaticSeedCandidateProvider} is used with the hosts
 * passed to the client constructor.
 */
public interface SeedCandidateProvider {
	/**
	 * Return the current seed candidates. Returned hosts are used as seeds, so they must carry the
	 * TLS name when TLS is enabled; a null TLS name is defaulted the same way as for configured seeds.
	 */
	List<Host> refreshSeedCandidates();

	/**
	 * Return true if the provider's candidates can change and must be refreshed periodically.
	 * Return false if the candidates are fixed, in which case the client schedules no periodic
	 * work for this provider.
	 */
	boolean needsPeriodicRefresh();

	/**
	 * Return true if {@link #refreshSeedCandidates()} honors a deadline: it either does no I/O or
	 * bounds all of its I/O so that it can return within a caller-supplied time limit. Only providers
	 * that return true are eligible to run on the cluster tend thread.
	 */
	boolean supportsDeadline();
}
