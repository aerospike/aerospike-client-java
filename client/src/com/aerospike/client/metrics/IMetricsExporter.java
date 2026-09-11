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

/**
 * Interface for exporting Aerospike client metrics to external systems.
 * <p>
 * Implementations can export metrics to OpenTelemetry, Prometheus, log files,
 * or any custom observability platform.
 * <p>
 * The exporter is responsible for its own initialization and cleanup.
 * The snapshot passed to {@link #export(MetricsSnapshot)} is immutable and
 * safe to retain, but exporters should avoid holding references longer than
 * necessary to reduce memory pressure.
 * <p>
 * Exporter calls are isolated per exporter. A failure in one exporter does
 * not prevent other exporters from receiving the same snapshot. Exporters
 * are invoked from the metrics thread, never from the tend thread or
 * command hot path.
 *
 * @see MetricsSnapshot
 * @see MetricsPolicy
 */
public interface IMetricsExporter {

	/**
	 * Export a metrics snapshot. Called periodically by the metrics thread
	 * based on the configured interval.
	 *
	 * @param snapshot immutable point-in-time snapshot of all client metrics
	 */
	void export(MetricsSnapshot snapshot);

	/**
	 * Release any resources held by this exporter (connections, threads, etc.).
	 */
	void close();
}
