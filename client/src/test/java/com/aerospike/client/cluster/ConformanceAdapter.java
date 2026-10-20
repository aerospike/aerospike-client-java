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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.aerospike.client.Host;
import com.aerospike.client.discovery.AddressTranslator;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.SeedCandidateContext;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.StaticMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;

/**
 * Reach package-private cluster seams for the conformance runner.
 */
public final class ConformanceAdapter {
	private static final int TEND_INTERVAL = 1000;

	/**
	 * Translate an advertised endpoint with the translator and miss mode the cluster selects from
	 * the policy.
	 */
	public static Host translate(ClientPolicy policy, Endpoint advertised) {
		AddressTranslator translator = (policy.addressTranslator != null)?
			policy.addressTranslator : new StaticMapAddressTranslator(policy.ipMap);

		TranslationMissMode onMiss = (policy.translationOnMiss != null)?
			policy.translationOnMiss : TranslationMissMode.PASS_THROUGH;

		return Cluster.translate(translator, onMiss, advertised);
	}

	/**
	 * Apply each discovery result in order, the first at initialization and each later one as a
	 * refresh, and return the published seed snapshot after each.
	 */
	public static List<Host[]> refreshSeeds(ClientPolicy policy, Host[] staticSeeds, List<Host[]> results) {
		policy.discoveryRefreshInterval = TEND_INTERVAL;
		policy.discoveryTendRefreshDeadline = Integer.MAX_VALUE;

		Iterator<Host[]> iter = results.iterator();
		SeedCandidateProvider provider = new SeedCandidateProvider() {
			@Override
			public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
				return List.of(iter.next());
			}

			@Override
			public boolean needsPeriodicRefresh() {
				return true;
			}

			@Override
			public boolean supportsDeadline() {
				return true;
			}
		};

		AtomicReference<Host[]> snapshot = new AtomicReference<>();
		SeedRefresher refresher = new SeedRefresher(provider, policy, staticSeeds, snapshot::set);
		List<Host[]> snapshots = new ArrayList<>(results.size());

		SeedCandidateContext ctx = new SeedCandidateContext(policy.clusterName, staticSeeds, 0);
		Host[] seeds = refresher.merge(provider.refreshSeedCandidates(ctx).toArray(new Host[0]), true);
		snapshot.set(seeds);
		refresher.start(seeds);
		snapshots.add(snapshot.get());

		for (int tendCount = 1; iter.hasNext(); tendCount++) {
			refresher.tend(tendCount, TEND_INTERVAL);
			snapshots.add(snapshot.get());
		}
		return snapshots;
	}
}
