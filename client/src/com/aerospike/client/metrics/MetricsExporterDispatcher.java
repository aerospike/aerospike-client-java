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
import java.util.Locale;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.aerospike.client.Log;
import com.aerospike.client.util.Util;

/**
 * Delivers snapshots to exporters with independent timeout and failure state.
 */
final class MetricsExporterDispatcher {
	private final MetricsPolicy policy;
	private final LongSupplier clock;
	private final TimeUnit timeoutUnit;
	private final List<ExporterRegistration> registrations;
	private volatile boolean running = true;

	MetricsExporterDispatcher(MetricsPolicy policy) {
		this(
			policy,
			System::currentTimeMillis,
			MetricsExporterDispatcher::newExecutor,
			TimeUnit.SECONDS
		);
	}

	MetricsExporterDispatcher(
		MetricsPolicy policy,
		LongSupplier clock,
		Supplier<ExecutorService> executorFactory,
		TimeUnit timeoutUnit
	) {
		this.policy = policy;
		this.clock = clock;
		this.timeoutUnit = timeoutUnit;
		List<ExporterRegistration> registrations = new ArrayList<>();

		for (IMetricsExporter exporter : policy.getExporters()) {
			registrations.add(new ExporterRegistration(exporter, executorFactory.get()));
		}
		this.registrations = Collections.unmodifiableList(registrations);
	}

	void dispatch(MetricsSnapshot snapshot) {
		for (ExporterRegistration registration : registrations) {
			if (!running) {
				return;
			}

			if (!isReady(registration)) {
				continue;
			}

			Future<?> future;

			try {
				future = registration.submit(
					() -> registration.exporter.export(snapshot));
			}
			catch (RejectedExecutionException ignored) {
				continue;
			}

			if (!awaitExport(registration, future)) {
				return;
			}
		}
	}

	void shutdown() {
		if (!running) {
			return;
		}

		running = false;

		for (ExporterRegistration registration : registrations) {
			registration.shutdown();
		}
	}

	private boolean isReady(ExporterRegistration registration) {
		if (!registration.suspended) {
			return true;
		}

		long elapsed = clock.getAsLong() - registration.suspendedAt;

		if (elapsed < TimeUnit.SECONDS.toMillis(policy.suspendRetryInterval)) {
			return false;
		}

		Log.info("Retrying suspended exporter: "
			+ exporterName(registration.exporter));
		return true;
	}

	/**
	 * Wait for one exporter invocation to finish.
	 *
	 * @return {@code true} when dispatch may continue to the remaining
	 * exporters, including after an exporter-specific failure; {@code false}
	 * when cancellation or interruption requires dispatch to stop
	 */
	private boolean awaitExport(
		ExporterRegistration registration,
		Future<?> future
	) {
		try {
			future.get(policy.exportTimeout, timeoutUnit);

			if (registration.suspended) {
				Log.info("Exporter " + exporterName(registration.exporter)
					+ " resumed after successful retry");
			}
			registration.reset();
			return true;
		}
		catch (TimeoutException e) {
			future.cancel(true);
			recordFailure(registration, " timed out after "
				+ policy.exportTimeout + timeoutSuffix() + ", snapshot dropped");
			return true;
		}
		catch (ExecutionException e) {
			// The task has already completed exceptionally, so cancellation
			// would have no effect.
			recordFailure(registration,
				" failed: " + Util.getErrorMessage(e.getCause()));
			return true;
		}
		catch (CancellationException ignored) {
			// Cancellation is expected during shutdown.
			return false;
		}
		catch (InterruptedException e) {
			future.cancel(true);

			if (running) {
				// Future.get() clears the interrupt status when it throws.
				// Restore an unexpected interrupt on the metrics thread.
				Thread.currentThread().interrupt();
			}
			return false;
		}
		finally {
			registration.clear(future);
		}
	}

	private void recordFailure(
		ExporterRegistration registration,
		String message
	) {
		registration.consecutiveFailures++;
		Log.warn("Exporter " + exporterName(registration.exporter) + message
			+ " (consecutive=" + registration.consecutiveFailures + ")");

		if (registration.consecutiveFailures >= policy.maxConsecutiveFailures) {
			registration.suspended = true;
			registration.suspendedAt = clock.getAsLong();
			Log.error("Exporter " + exporterName(registration.exporter)
				+ " suspended after " + registration.consecutiveFailures
				+ " consecutive failures");
		}
	}

	private static String exporterName(IMetricsExporter exporter) {
		String name = exporter.getClass().getSimpleName();
		return name.isEmpty() ? exporter.getClass().getName() : name;
	}

	private String timeoutSuffix() {
		return timeoutUnit == TimeUnit.SECONDS
			? "s"
			: " " + timeoutUnit.name().toLowerCase(Locale.ROOT);
	}

	private static ExecutorService newExecutor() {
		return Executors.newSingleThreadExecutor(runnable -> {
			Thread thread = new Thread(runnable, "aerospike-metrics-dispatch");
			thread.setDaemon(true);
			return thread;
		});
	}

	private static final class ExporterRegistration {
		final IMetricsExporter exporter;
		final ExecutorService executor;
		int consecutiveFailures;
		boolean suspended;
		long suspendedAt;
		Future<?> inFlight;

		ExporterRegistration(
			IMetricsExporter exporter,
			ExecutorService executor
		) {
			this.exporter = exporter;
			this.executor = executor;
		}

		synchronized Future<?> submit(Runnable task) {
			inFlight = executor.submit(task);
			return inFlight;
		}

		synchronized void shutdown() {
			if (inFlight != null) {
				inFlight.cancel(true);
			}
			executor.shutdownNow();
		}

		synchronized void clear(Future<?> future) {
			if (inFlight == future) {
				inFlight = null;
			}
		}

		void reset() {
			consecutiveFailures = 0;
			suspended = false;
			suspendedAt = 0;
		}
	}
}
