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

import com.aerospike.client.Log;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.util.Util;

/**
 * Schedules periodic snapshot capture and exporter delivery outside the tend
 * thread and command hot path.
 */
public class MetricsExporterThread extends Thread {
	private final MetricsPolicy policy;
	private final MetricsSnapshotBuilder snapshotBuilder;
	private final MetricsExporterDispatcher dispatcher;
	private volatile boolean running = true;

	public MetricsExporterThread(Cluster cluster, MetricsPolicy policy) {
		super("aerospike-metrics-exporter");
		setDaemon(true);

		policy.validateExporterSettings();
		this.policy = policy;
		this.snapshotBuilder = new MetricsSnapshotBuilder(cluster, policy);
		this.dispatcher = new MetricsExporterDispatcher(policy);
	}

	@Override
	public void run() {
		Log.info("Metrics exporter thread started, interval=" + policy.interval
			+ "s, exporters=" + policy.getExporters().size());

		while (sleepUntilNextSnapshot()) {
			MetricsSnapshot snapshot;

			try {
				snapshot = snapshotBuilder.build();
			}
			catch (Exception e) {
				Log.warn("Failed to capture metrics snapshot: " + Util.getErrorMessage(e));
				continue;
			}

			dispatcher.dispatch(snapshot);
		}

		Log.info("Metrics exporter thread stopped");
	}

	/**
	 * Signal the metrics exporter thread and all exporter dispatchers to stop.
	 * This method is safe to call more than once.
	 */
	public void shutdown() {
		if (!running) {
			return;
		}

		running = false;
		dispatcher.shutdown();
		interrupt();
	}

	private boolean sleepUntilNextSnapshot() {
		if (!running) {
			return false;
		}

		try {
			Thread.sleep(policy.interval * 1000L);
			return running;
		}
		catch (InterruptedException e) {
			if (running) {
				Thread.currentThread().interrupt();
			}
			return false;
		}
	}
}
