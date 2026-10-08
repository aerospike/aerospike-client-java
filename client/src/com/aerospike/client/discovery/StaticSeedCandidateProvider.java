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
 * Seed candidate provider that returns a fixed list of hosts. This provider does no I/O and
 * needs no periodic refresh.
 */
public final class StaticSeedCandidateProvider implements SeedCandidateProvider {
	private final List<Host> hosts;

	/**
	 * Initialize provider with fixed seed hosts.
	 */
	public StaticSeedCandidateProvider(Host... hosts) {
		this.hosts = List.of(hosts);
	}

	/**
	 * Return the fixed seed hosts.
	 */
	@Override
	public List<Host> refreshSeedCandidates() {
		return hosts;
	}

	/**
	 * Return false. The seed hosts never change.
	 */
	@Override
	public boolean needsPeriodicRefresh() {
		return false;
	}

	/**
	 * Return true. The provider does no I/O.
	 */
	@Override
	public boolean supportsDeadline() {
		return true;
	}
}
