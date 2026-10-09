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
package com.aerospike.client.cluster;

import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Util;

/**
 * Schedule periodic seed candidate refreshes on the cluster tend thread.
 */
final class SeedRefresher {
	static final int MAX_OVERRUNS = 3;

	private final SeedCandidateProvider provider;
	private final Consumer<Host[]> publisher;
	private final Host[] staticSeeds;
	private final SeedMergePolicy mergePolicy;
	private final boolean periodic;
	private final int refreshInterval;
	private final int tendRefreshDeadline;
	private Host[] initSeeds;
	private volatile int overrunCount;
	private volatile boolean tripped;

	SeedRefresher(SeedCandidateProvider provider, ClientPolicy policy, Consumer<Host[]> publisher) {
		this(provider, policy, new Host[0], publisher);
	}

	SeedRefresher(SeedCandidateProvider provider, ClientPolicy policy, Host[] staticSeeds, Consumer<Host[]> publisher) {
		this.provider = provider;
		this.publisher = publisher;
		this.staticSeeds = staticSeeds.clone();
		this.mergePolicy = (policy.discoveryMergePolicy != null)?
			policy.discoveryMergePolicy : SeedMergePolicy.MERGE;
		this.periodic = provider.needsPeriodicRefresh();
		this.refreshInterval = policy.discoveryRefreshInterval;
		this.tendRefreshDeadline = policy.discoveryTendRefreshDeadline;

		if (periodic && ! provider.supportsDeadline()) {
			throw new AerospikeException(
				"A seed candidate provider that needs periodic refresh must support a deadline");
		}
	}

	void validate(int tendInterval) {
		if (! periodic) {
			return;
		}

		if (refreshInterval < tendInterval) {
			throw new AerospikeException("Discovery refresh interval " + refreshInterval +
				" must be greater or equal to the tend interval " + tendInterval);
		}

		if (tendRefreshDeadline <= 0 || tendRefreshDeadline > tendInterval / 2) {
			throw new AerospikeException("Invalid discovery tend refresh deadline: " + tendRefreshDeadline +
				". Must be > 0 and <= tend interval / 2 (" + (tendInterval / 2) + ")");
		}
	}

	void start(Host[] initSeeds) {
		this.initSeeds = initSeeds;
	}

	Host[] merge(Host[] discovered, boolean init) {
		switch (mergePolicy) {
		case REPLACE:
			if (! init) {
				return distinct(discovered);
			}
			return distinct(staticSeeds, discovered);

		case DISCOVERY_ONLY:
			return distinct((discovered.length > 0)? discovered : staticSeeds);

		default:
			return distinct(staticSeeds, discovered);
		}
	}

	private static Host[] distinct(Host[]... lists) {
		LinkedHashSet<Host> set = new LinkedHashSet<>();

		for (Host[] list : lists) {
			for (Host host : list) {
				set.add(host);
			}
		}
		return set.toArray(new Host[set.size()]);
	}

	void tend(int tendCount, int tendInterval) {
		if (! periodic || tripped || tendCount % (refreshInterval / tendInterval) != 0) {
			return;
		}

		long begin = System.nanoTime();
		Host[] hosts = refresh();
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		if (elapsed > tendRefreshDeadline) {
			int count = ++overrunCount;

			if (Log.warnEnabled()) {
				Log.warn("Seed candidate refresh took " + elapsed + "ms, exceeding tend deadline " +
					tendRefreshDeadline + "ms. Consecutive overruns: " + count);
			}

			if (count >= MAX_OVERRUNS) {
				tripped = true;
				publish(initSeeds);

				if (Log.errorEnabled()) {
					Log.error("Seed candidate provider exceeded tend deadline " + count +
						" consecutive times. Provider disabled for this client and initial seeds restored");
				}
			}
			return;
		}

		overrunCount = 0;

		if (hosts != null) {
			Host[] seeds = merge(hosts, false);

			if (seeds.length > 0) {
				publish(seeds);
			}
		}
	}

	private Host[] refresh() {
		try {
			return provider.refreshSeedCandidates().toArray(new Host[0]);
		}
		catch (Throwable e) {
			if (Log.warnEnabled()) {
				Log.warn("Seed candidate refresh failed: " + Util.getErrorMessage(e));
			}
			return null;
		}
	}

	private void publish(Host[] hosts) {
		try {
			publisher.accept(hosts);
		}
		catch (Throwable e) {
			if (Log.warnEnabled()) {
				Log.warn("Seed candidate publish failed: " + Util.getErrorMessage(e));
			}
		}
	}

	int getOverrunCount() {
		return overrunCount;
	}

	boolean isTripped() {
		return tripped;
	}
}
