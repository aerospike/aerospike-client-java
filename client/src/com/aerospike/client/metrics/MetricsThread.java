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
package com.aerospike.client.metrics;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.Log;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.util.Util;

/**
 * Schedules periodic snapshot capture and exporter delivery outside the tend
 * thread and command hot path.
 */
public final class MetricsThread extends Thread {
	/**
	 * Generates unique metrics thread names for diagnostics.
	 */
	private static final AtomicLong MetricsThreadSequence = new AtomicLong();

	/** Export interval and exporter failure policy for this thread. */
	private final MetricsPolicy policy;

	/** Creates one immutable point-in-time snapshot per export cycle. */
	private final MetricsSnapshotFactory snapshotFactory;

	/** Invokes eligible exporters and maintains their independent failure state. */
	private final MetricsExporterManager exporterManager;

	/** Cluster name included in lifecycle logs. */
	private final String clusterName;

	/**
	 * Records a permanent stop request and wakes this metrics thread if it is
	 * waiting for the next export interval.
	 */
	private final CountDownLatch metricsThreadStopLatch = new CountDownLatch(1);

	/**
	 * Initialize a metrics thread for the supplied exporters.
	 */
	public MetricsThread(
		Cluster cluster,
		MetricsPolicy policy,
		List<IMetricsExporter> exporters
	) {
		super("aerospike-metrics-" + MetricsThreadSequence.incrementAndGet());
		setDaemon(true);

		this.policy = policy;
		this.snapshotFactory = new MetricsSnapshotFactory(cluster, policy);
		this.exporterManager = new MetricsExporterManager(
			policy, exporters, System::nanoTime);

		String name = cluster.getClusterName();
		this.clusterName = name == null ? "" : name;
	}

	@Override
	public void run() {
		long exportIntervalMillis = Math.max(policy.exportInterval.toMillis(), 1L);
		Log.info("Metrics thread " + getName() + " started for cluster " +
			clusterName + ", interval=" + policy.exportInterval);

		try {
			while (true) {
				try {
					// await() returns true when disable/close requests a stop, or
					// false when the export interval expires.
					if (metricsThreadStopLatch.await(
						exportIntervalMillis, TimeUnit.MILLISECONDS
					)) {
						exportSnapshot(false);
						return;
					}
				}
				catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					Log.warn("Metrics thread " + getName() +
						" interrupted unexpectedly for cluster " + clusterName);
					return;
				}
				exportSnapshot(true);
			}
		}
		finally {
			Log.info("Metrics thread " + getName() + " stopped for cluster " +
				clusterName);
		}
	}

	/**
	 * Signals the metrics thread to stop. If it is waiting for the next interval,
	 * the signal wakes it immediately. If an export is in progress, that export
	 * completes normally before the final export is performed.
	 * <p>
	 * This method blocks until the final export and thread termination complete.
	 */
	public void stopAndAwaitFinalExport() {
		metricsThreadStopLatch.countDown();
		boolean callerWasInterrupted = false;

		while (isAlive()) {
			try {
				join();
			}
			catch (InterruptedException ie) {
				callerWasInterrupted = true;
			}
		}

		if (callerWasInterrupted) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Build and export one snapshot on this metrics thread.
	 *
	 * @param metricsEnabled whether metrics are enabled for this snapshot
	 */
	private void exportSnapshot(boolean metricsEnabled) {
		if (!exporterManager.hasRunnableExporter()) {
			return;
		}

		try {
			exporterManager.export(snapshotFactory.create(metricsEnabled));
		}
		catch (Throwable e) {
			Log.warn("Failed to capture or export metrics snapshot: " +
				Util.getErrorMessage(e));
		}
	}
}
