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
 * Exporters are invoked sequentially from the metrics thread, never from the tend thread,
 * command hot path or disable/client-close caller thread.
 * The client does not close application-owned exporters.
 *
 * @see MetricsSnapshot
 * @see MetricsPolicy
 */
public interface IMetricsExporter {

    /**
     * Exports client metrics. Called periodically and once more during
     * disable/client close by the metrics thread. Periodic snapshots report
     * metrics enabled; the final snapshot reports metrics disabled.
     *
     * @param snapshot immutable point-in-time snapshot of all client metrics
     */
    void export(MetricsSnapshot snapshot);

}
