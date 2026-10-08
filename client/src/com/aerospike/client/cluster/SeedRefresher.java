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

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.DiscoveryExecutionMode;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Util;

/**
 * Schedule periodic seed candidate refreshes in thread or tend mode.
 */
final class SeedRefresher implements Runnable {
	static final int MAX_OVERRUNS = 3;

	private final SeedCandidateProvider provider;
	private final Consumer<Host[]> publisher;
	private final DiscoveryExecutionMode mode;
	private final boolean periodic;
	private final int refreshInterval;
	private final int tendRefreshDeadline;
	private final int refreshTimeout;
	private final Object lock = new Object();
	private Host[] initSeeds;
	private Thread thread;
	private volatile boolean valid;
	private volatile int overrunCount;
	private volatile boolean tripped;

	SeedRefresher(SeedCandidateProvider provider, ClientPolicy policy, Consumer<Host[]> publisher) {
		this.provider = provider;
		this.publisher = publisher;
		this.mode = (policy.discoveryExecutionMode != null)?
			policy.discoveryExecutionMode : DiscoveryExecutionMode.THREAD;
		this.periodic = provider.needsPeriodicRefresh();
		this.refreshInterval = policy.discoveryRefreshInterval;
		this.tendRefreshDeadline = policy.discoveryTendRefreshDeadline;
		this.refreshTimeout = policy.discoveryRefreshTimeout;

		if (mode == DiscoveryExecutionMode.TEND && ! provider.supportsDeadline()) {
			throw new AerospikeException("Discovery execution mode " + mode +
				" requires a seed candidate provider that supports a deadline");
		}
	}

	void validate(int tendInterval) {
		if (periodic && refreshInterval < tendInterval) {
			throw new AerospikeException("Discovery refresh interval " + refreshInterval +
				" must be greater or equal to the tend interval " + tendInterval);
		}

		if (mode == DiscoveryExecutionMode.TEND &&
			(tendRefreshDeadline <= 0 || tendRefreshDeadline > tendInterval / 2)) {
			throw new AerospikeException("Invalid discovery tend refresh deadline: " + tendRefreshDeadline +
				". Must be > 0 and <= tend interval / 2 (" + (tendInterval / 2) + ")");
		}
	}

	void start(Host[] initSeeds) {
		this.initSeeds = initSeeds;

		if (periodic && mode == DiscoveryExecutionMode.THREAD) {
			valid = true;
			thread = new Thread(this);
			thread.setName("discovery");
			thread.setDaemon(true);
			thread.start();
		}
	}

	void tend(int tendCount, int tendInterval) {
		if (! periodic || mode != DiscoveryExecutionMode.TEND || tripped ||
			tendCount % (refreshInterval / tendInterval) != 0) {
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
			publish(hosts);
		}
	}

	public void run() {
		while (valid) {
			if (! waitInterval()) {
				break;
			}

			long begin = System.nanoTime();
			Host[] hosts = refresh();
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

			if (elapsed > refreshTimeout) {
				if (Log.warnEnabled()) {
					Log.warn("Seed candidate refresh took " + elapsed + "ms, exceeding timeout " +
						refreshTimeout + "ms. Result discarded");
				}
				continue;
			}

			if (hosts != null && valid) {
				publish(hosts);
			}
		}
	}

	private boolean waitInterval() {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(refreshInterval);

		synchronized (lock) {
			while (valid) {
				long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());

				if (remaining <= 0) {
					return true;
				}

				try {
					lock.wait(remaining);
				}
				catch (InterruptedException ie) {
					return false;
				}
			}
		}
		return false;
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

	void close() {
		valid = false;

		synchronized (lock) {
			lock.notifyAll();
		}
	}

	Thread getThread() {
		return thread;
	}

	int getOverrunCount() {
		return overrunCount;
	}

	boolean isTripped() {
		return tripped;
	}
}
