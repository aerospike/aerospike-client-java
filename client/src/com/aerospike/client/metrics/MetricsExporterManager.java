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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import com.aerospike.client.Log;
import com.aerospike.client.util.Util;

/**
 * Invokes registered exporters sequentially and manages each exporter's
 * independent failure, suspension and retry state.
 */
final class MetricsExporterManager {
	/** Failure threshold and suspended-exporter retry interval. */
	private final MetricsPolicy policy;

	/** Monotonic nanosecond source used for exporter retry scheduling. */
	private final LongSupplier nanoTimeSupplier;

	/** Immutable exporter order with mutable health state per exporter. */
	private final List<ExporterState> exporterStates;

	/**
	 * Initialize immutable exporter registrations with an injectable monotonic
	 * time source for deterministic retry scheduling.
	 */
	MetricsExporterManager(
		MetricsPolicy policy,
		List<IMetricsExporter> exporters,
		LongSupplier nanoTimeSupplier
	) {
		this.policy = policy;
		this.nanoTimeSupplier = nanoTimeSupplier;
		List<ExporterState> states = new ArrayList<>(exporters.size());

		for (IMetricsExporter exporter : exporters) {
			states.add(new ExporterState(exporter));
		}
		this.exporterStates = Collections.unmodifiableList(states);
	}

	/**
	 * Export one snapshot to every currently eligible exporter in registration
	 * order. A failure does not prevent later exporters from running.
	 */
	void export(MetricsSnapshot snapshot) {
		for (ExporterState state : exporterStates) {
			if (!isReady(state)) {
				continue;
			}

			try {
				state.exporter.export(snapshot);

				if (state.suspended) {
					Log.info("Exporter " + exporterName(state.exporter) +
						" resumed after successful retry");
				}
				state.reset();
			}
			catch (Throwable e) {
				recordFailure(state, e);
			}
		}
	}

	/**
	 * Return true when at least one exporter can receive a snapshot now.
	 */
	boolean hasRunnableExporter() {
		for (ExporterState state : exporterStates) {
			if (!state.suspended || retryDue(state)) {
				return true;
			}
		}
		return false;
	}

	private boolean isReady(ExporterState state) {
		if (!state.suspended) {
			return true;
		}

		if (!retryDue(state)) {
			return false;
		}

		Log.info("Retrying suspended exporter: " + exporterName(state.exporter));
		return true;
	}

	private boolean retryDue(ExporterState state) {
		long elapsedNanos = nanoTimeSupplier.getAsLong() - state.suspendedAtNanos;
		return elapsedNanos >= TimeUnit.SECONDS.toNanos(policy.suspendRetryInterval);
	}

	private void recordFailure(ExporterState state, Throwable cause) {
		state.consecutiveFailures++;
		Log.warn("Exporter " + exporterName(state.exporter) + " failed: " +
			Util.getErrorMessage(cause) + " (consecutive=" +
			state.consecutiveFailures + ")");

		if (state.consecutiveFailures >= policy.maxConsecutiveFailures) {
			state.suspended = true;
			state.suspendedAtNanos = nanoTimeSupplier.getAsLong();
			Log.error("Exporter " + exporterName(state.exporter) +
				" suspended after " + state.consecutiveFailures +
				" consecutive failures");
		}
	}

	private static String exporterName(IMetricsExporter exporter) {
		String name = exporter.getClass().getSimpleName();
		return name.isEmpty() ? exporter.getClass().getName() : name;
	}

	/**
	 * Stores the independent health and retry state for one exporter.
	 */
	private static final class ExporterState {
		/** Application-owned or client-owned exporter implementation. */
		final IMetricsExporter exporter;

		/** Failures since the exporter's most recent successful call. */
		int consecutiveFailures;

		/** Whether normal calls are skipped until the retry interval elapses. */
		boolean suspended;

		/** Monotonic nanosecond value when the most recent suspension began. */
		long suspendedAtNanos;

		ExporterState(IMetricsExporter exporter) {
			this.exporter = exporter;
		}

		void reset() {
			consecutiveFailures = 0;
			suspended = false;
			suspendedAtNanos = 0;
		}
	}
}
