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

import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
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
	private final Supplier<ExecutorService> executorFactory;
	private final TimeUnit timeoutUnit;
	private final Map<IMetricsExporter, ExporterState> states;
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
		this.executorFactory = executorFactory;
		this.timeoutUnit = timeoutUnit;
		this.states = new IdentityHashMap<>();

		for (IMetricsExporter exporter : policy.getExporters()) {
			states.put(exporter, new ExporterState(executorFactory.get()));
		}
	}

	void dispatch(MetricsSnapshot snapshot) {
		for (IMetricsExporter exporter : policy.getExporters()) {
			if (!running) {
				return;
			}

			ExporterState state = states.get(exporter);

			if (state == null) {
				state = addState(exporter);
			}

			if (state == null || !isReady(exporter, state)) {
				continue;
			}

			Future<?> future;

			try {
				future = state.submit(() -> exporter.export(snapshot));
			}
			catch (RejectedExecutionException ignored) {
				continue;
			}

			await(exporter, state, future);
		}
	}

	void shutdown() {
		if (!running) {
			return;
		}

		running = false;

		synchronized (states) {
			for (ExporterState state : states.values()) {
				state.shutdown();
			}
		}
	}

	private ExporterState addState(IMetricsExporter exporter) {
		synchronized (states) {
			if (!running) {
				return null;
			}

			ExporterState state = states.get(exporter);

			if (state == null) {
				state = new ExporterState(executorFactory.get());
				states.put(exporter, state);
			}
			return state;
		}
	}

	private boolean isReady(IMetricsExporter exporter, ExporterState state) {
		if (!state.suspended) {
			return true;
		}

		long elapsed = clock.getAsLong() - state.suspendedAt;

		if (elapsed < TimeUnit.SECONDS.toMillis(policy.suspendRetryInterval)) {
			return false;
		}

		Log.info("Retrying suspended exporter: " + exporterName(exporter));
		return true;
	}

	private void await(
		IMetricsExporter exporter,
		ExporterState state,
		Future<?> future
	) {
		try {
			future.get(policy.exportTimeout, timeoutUnit);

			if (state.suspended) {
				Log.info("Exporter " + exporterName(exporter)
					+ " resumed after successful retry");
			}
			state.reset();
		}
		catch (TimeoutException e) {
			future.cancel(true);
			recordFailure(exporter, state, " timed out after "
				+ policy.exportTimeout + timeoutSuffix() + ", snapshot dropped");
		}
		catch (ExecutionException e) {
			recordFailure(exporter, state,
				" failed: " + Util.getErrorMessage(e.getCause()));
		}
		catch (CancellationException ignored) {
			// Cancellation is expected during shutdown.
		}
		catch (InterruptedException e) {
			future.cancel(true);

			if (running) {
				Thread.currentThread().interrupt();
			}
		}
		finally {
			state.clear(future);
		}
	}

	private void recordFailure(
		IMetricsExporter exporter,
		ExporterState state,
		String message
	) {
		state.consecutiveFailures++;
		Log.warn("Exporter " + exporterName(exporter) + message
			+ " (consecutive=" + state.consecutiveFailures + ")");

		if (state.consecutiveFailures >= policy.maxConsecutiveFailures) {
			state.suspended = true;
			state.suspendedAt = clock.getAsLong();
			Log.error("Exporter " + exporterName(exporter) + " suspended after "
				+ state.consecutiveFailures + " consecutive failures");
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

	private static final class ExporterState {
		final ExecutorService executor;
		int consecutiveFailures;
		boolean suspended;
		long suspendedAt;
		volatile Future<?> inFlight;

		ExporterState(ExecutorService executor) {
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
