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
import com.aerospike.client.ResultCode;
import com.aerospike.client.configuration.serializers.staticconfig.StaticDiscoveryConfig;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider;
import com.aerospike.client.discovery.DiscoveryExecutionMode;
import com.aerospike.client.discovery.DnsSeedCandidateProvider;
import com.aerospike.client.discovery.SeedCandidateContext;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.discovery.StaticSeedCandidateProvider;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Util;

/**
 * Schedule periodic seed candidate refreshes on a discovery thread or on the cluster tend thread.
 */
final class SeedRefresher {
	static final int MAX_OVERRUNS = 3;

	private final SeedCandidateProvider provider;
	private final Consumer<Host[]> publisher;
	private final String clusterName;
	private final Host[] staticSeeds;
	private final SeedMergePolicy mergePolicy;
	private final boolean periodic;
	private final boolean threaded;
	private final int refreshInterval;
	private final int tendRefreshDeadline;
	private final int refreshTimeout;
	private final Object lock = new Object();
	private Host[] initSeeds;
	private volatile int overrunCount;
	private volatile boolean tripped;
	private volatile int lastTendCount;
	private long lastUrgent;
	private boolean urgentRequested;

	// Discovery thread state, guarded by lock.
	private Thread thread;
	private Host[] firstHosts;
	private Throwable firstError;
	private boolean firstDone;
	private boolean firstAbandoned;
	private boolean started;
	private boolean urgent;
	private boolean stopped;

	SeedRefresher(SeedCandidateProvider provider, ClientPolicy policy, Consumer<Host[]> publisher) {
		this(provider, policy, new Host[0], publisher);
	}

	SeedRefresher(SeedCandidateProvider provider, ClientPolicy policy, Host[] staticSeeds, Consumer<Host[]> publisher) {
		this.provider = provider;
		this.publisher = publisher;
		this.clusterName = policy.clusterName;
		this.staticSeeds = staticSeeds.clone();
		this.mergePolicy = (policy.discoveryMergePolicy != null)?
			policy.discoveryMergePolicy : SeedMergePolicy.MERGE;
		this.periodic = provider.needsPeriodicRefresh();
		this.threaded = periodic && policy.discoveryExecutionMode != DiscoveryExecutionMode.TEND;
		this.refreshInterval = policy.discoveryRefreshInterval;
		this.tendRefreshDeadline = policy.discoveryTendRefreshDeadline;
		this.refreshTimeout = policy.discoveryRefreshTimeout;

		if (periodic && ! threaded && ! provider.supportsDeadline()) {
			throw new AerospikeException(
				"A seed candidate provider that needs periodic refresh must support a deadline in discovery execution mode tend");
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

		if (threaded) {
			if (refreshTimeout <= 0) {
				throw new AerospikeException("Invalid discovery refresh timeout: " + refreshTimeout + ". Must be > 0");
			}
			validateTimeout(refreshTimeout, "discovery refresh timeout");
			return;
		}

		if (tendRefreshDeadline <= 0 || tendRefreshDeadline > tendInterval / 2) {
			throw new AerospikeException("Invalid discovery tend refresh deadline: " + tendRefreshDeadline +
				". Must be > 0 and <= tend interval / 2 (" + (tendInterval / 2) + ")");
		}
		validateTimeout(tendRefreshDeadline, "discovery tend refresh deadline");
	}

	private void validateTimeout(int limit, String limitName) {
		if (provider instanceof DnsSeedCandidateProvider) {
			int timeout = ((DnsSeedCandidateProvider)provider).getTimeout();

			if (timeout > limit) {
				throw new AerospikeException("DNS seed provider timeout " + timeout +
					" must be less or equal to the " + limitName + " " + limit);
			}
		}

		if (provider instanceof AwsEc2TagSeedCandidateProvider) {
			int timeout = ((AwsEc2TagSeedCandidateProvider)provider).getTimeout();

			if (timeout > limit) {
				throw new AerospikeException("AWS EC2 seed provider timeout " + timeout +
					" must be less or equal to the " + limitName + " " + limit);
			}
		}
	}

	/**
	 * Return the initial seed list: the first refresh merged with the static seeds. In thread mode,
	 * start the discovery thread, which makes the first refresh, and wait for it at most the refresh
	 * timeout. A first refresh that completes later is published when {@link #start} has been called.
	 *
	 * @throws AerospikeException	with {@link ResultCode#SEED_DISCOVERY_FAILED} when the first refresh
	 * fails or does not complete in time and no seed host is left
	 */
	Host[] discover() {
		Host[] discovered = null;
		Throwable error = null;

		if (threaded) {
			Thread t = new Thread(this::run);
			t.setName("discovery");
			t.setDaemon(true);

			synchronized (lock) {
				thread = t;
				t.start();

				long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(refreshTimeout);
				long remaining;

				try {
					while (! firstDone && (remaining = deadline - System.nanoTime()) > 0) {
						TimeUnit.NANOSECONDS.timedWait(lock, remaining);
					}
				}
				catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}

				if (firstDone) {
					discovered = firstHosts;
					error = firstError;
				}
				else {
					firstAbandoned = true;
					error = new AerospikeException("First refresh did not complete within the discovery refresh timeout " +
						refreshTimeout + "ms");
				}
			}
		}
		else {
			try {
				discovered = candidates(0);
			}
			catch (Throwable e) {
				error = e;
			}
		}

		if (error != null) {
			discovered = null;

			if (Log.warnEnabled()) {
				Log.warn("Seed candidate provider " + providerName(provider) + " failed: " + Util.getErrorMessage(error));
			}
		}

		Host[] hosts = merge((discovered != null)? discovered : new Host[0], true);

		if (error != null && hosts.length == 0) {
			throw new AerospikeException(ResultCode.SEED_DISCOVERY_FAILED, "Seed candidate provider " +
				providerName(provider) + " failed and no seed host is left: " + error, error);
		}
		return hosts;
	}

	void start(Host[] initSeeds) {
		this.initSeeds = initSeeds;

		if (threaded) {
			synchronized (lock) {
				started = true;
				lock.notifyAll();
			}
		}
	}

	/**
	 * Ask the discovery thread to refresh now, without waiting for it. Called on the tend thread when
	 * no nodes are active. Honored at most once per refresh interval; does nothing in tend mode.
	 */
	void requestRefresh() {
		if (! threaded) {
			return;
		}

		long now = System.nanoTime();

		if (urgentRequested && now - lastUrgent < TimeUnit.MILLISECONDS.toNanos(refreshInterval)) {
			return;
		}

		urgentRequested = true;
		lastUrgent = now;

		synchronized (lock) {
			urgent = true;
			lock.notifyAll();
		}
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
		if (threaded) {
			lastTendCount = tendCount;
			return;
		}

		if (! periodic || tripped || tendCount % (refreshInterval / tendInterval) != 0) {
			return;
		}

		long begin = System.nanoTime();
		Host[] hosts = refresh(tendCount);
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

	private void run() {
		try {
			long begin = System.nanoTime();
			Host[] hosts = null;
			Throwable error = null;

			try {
				hosts = candidates(0);
			}
			catch (Throwable e) {
				error = e;
			}

			boolean late;

			synchronized (lock) {
				late = firstAbandoned;
				firstHosts = hosts;
				firstError = error;
				firstDone = true;
				lock.notifyAll();
			}
			checkOverrun(begin);

			if (late && error != null && Log.warnEnabled()) {
				Log.warn("Seed candidate refresh failed: " + Util.getErrorMessage(error));
			}

			if (! awaitStart()) {
				return;
			}

			if (late && hosts != null) {
				publishMerged(hosts, true);
			}

			long interval = TimeUnit.MILLISECONDS.toNanos(refreshInterval);
			long next = begin + interval;

			while (awaitRefresh(next)) {
				begin = System.nanoTime();
				next = begin + interval;
				hosts = refresh(lastTendCount);
				checkOverrun(begin);

				if (hosts != null) {
					publishMerged(hosts, false);
				}
			}
		}
		catch (InterruptedException ie) {
		}
		finally {
			closeProvider();
		}
	}

	private boolean awaitStart() throws InterruptedException {
		synchronized (lock) {
			while (! started && ! stopped) {
				lock.wait();
			}
			return ! stopped;
		}
	}

	private boolean awaitRefresh(long next) throws InterruptedException {
		synchronized (lock) {
			long remaining;

			while (! stopped && ! urgent && (remaining = next - System.nanoTime()) > 0) {
				TimeUnit.NANOSECONDS.timedWait(lock, remaining);
			}
			urgent = false;
			return ! stopped;
		}
	}

	private void checkOverrun(long begin) {
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		if (elapsed > refreshTimeout) {
			int count = ++overrunCount;

			if (Log.warnEnabled()) {
				Log.warn("Seed candidate refresh took " + elapsed + "ms, exceeding refresh timeout " +
					refreshTimeout + "ms. Overruns: " + count);
			}
		}
	}

	private void publishMerged(Host[] hosts, boolean init) {
		publish(merge(hosts, init));
	}

	private Host[] candidates(int tendCount) {
		SeedCandidateContext ctx = new SeedCandidateContext(clusterName, staticSeeds, tendCount);
		return provider.refreshSeedCandidates(ctx).toArray(new Host[0]);
	}

	private Host[] refresh(int tendCount) {
		try {
			return candidates(tendCount);
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

	/**
	 * Close the provider. Call once, after the last {@link #tend} call has returned. In thread mode,
	 * stop the discovery thread instead and wait at most the refresh timeout for a refresh in flight;
	 * the thread closes the provider as it exits, after that refresh has returned.
	 */
	void close() {
		if (threaded) {
			Thread t;

			synchronized (lock) {
				if (stopped) {
					return;
				}
				t = stop();
			}

			if (t != null) {
				awaitExit(t);
				return;
			}
		}
		closeProvider();
	}

	private Thread stop() {
		stopped = true;
		lock.notifyAll();
		return thread;
	}

	private void awaitExit(Thread t) {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(refreshTimeout);
		boolean interrupted = false;
		long remaining;

		while (t.isAlive() && (remaining = deadline - System.nanoTime()) > 0) {
			try {
				TimeUnit.NANOSECONDS.timedJoin(t, remaining);
			}
			catch (InterruptedException ie) {
				interrupted = true;
			}
		}

		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Stop the discovery thread when client construction fails, without waiting for a refresh in
	 * flight. Does nothing in tend mode.
	 */
	void abort() {
		if (threaded) {
			boolean idle;

			synchronized (lock) {
				idle = ! stopped && stop() == null;
			}

			if (idle) {
				closeProvider();
			}
		}
	}

	private void closeProvider() {
		try {
			provider.close();
		}
		catch (Throwable e) {
			if (Log.warnEnabled()) {
				Log.warn("Seed candidate provider " + providerName(provider) + " close failed: " +
					Util.getErrorMessage(e));
			}
		}
	}

	static String providerName(SeedCandidateProvider provider) {
		if (provider instanceof StaticSeedCandidateProvider) {
			return StaticDiscoveryConfig.STATIC;
		}
		if (provider instanceof DnsSeedCandidateProvider) {
			return StaticDiscoveryConfig.DNS;
		}
		if (provider instanceof AwsEc2TagSeedCandidateProvider) {
			return StaticDiscoveryConfig.AWS_TAGS;
		}
		return provider.getClass().getName();
	}

	int getOverrunCount() {
		return overrunCount;
	}

	boolean isTripped() {
		return tripped;
	}

	boolean isThreaded() {
		return threaded;
	}

	Thread getThread() {
		synchronized (lock) {
			return thread;
		}
	}
}
