/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import com.aerospike.client.policy.ClientPolicy;

/**
 * Client periodic metrics configuration.
 */
public final class MetricsPolicy {
	/**
	 * Listener that handles metrics notification events. The default listener implementation
	 * writes the metrics snapshot to a file which will later be read and forwarded to
	 * OpenTelemetry by a separate offline application.
	 * <p>
	 * The listener could be overridden to send the metrics snapshot directly to OpenTelemetry.
	 */
	public MetricsListener listener;

	/**
	 * Directory path to write metrics log files for listeners that write logs.
	 * <p>
	 * Default: <current directory>
	 */
	public String reportDir = ".";

	/**
	 * Metrics file size soft limit in bytes for listeners that write logs.
	 * <p>
	 * When reportSizeLimit is reached or exceeded, the current metrics file is closed and a new
	 * metrics file is created with a new timestamp. If reportSizeLimit is zero, the matrics file
	 * size is unbounded and the file will only be closed when
	 * {@link com.aerospike.client.AerospikeClient#disableMetrics()} or
	 * {@link com.aerospike.client.AerospikeClient#close()} is called.
	 * <p>
	 * Default: 0
	 */
	public long reportSizeLimit = 0;

	/**
	 * Number of cluster tend iterations between metrics notification events. One tend iteration
	 * is defined as {@link ClientPolicy#tendInterval} (default 1 second) plus the time to tend all
	 * nodes.
	 * <p>
	 * Default: 30
	 */
	public int interval = 30;

	/**
	 * Number of elapsed time range buckets in latency histograms.
	 * <p>
	 * Default: 5
	 */
	public int latencyColumns = 5;

	/**
	 * Power of 2 multiple between each range bucket in latency histograms starting at column 3. The bucket units
	 * are in milliseconds. The first 2 buckets are "<=1ms" and ">1ms". Examples:
	 * <pre>{@code
	 * // latencyColumns=5 latencyShift=3
	 * <=1ms >1ms >8ms >64ms >512ms
	 *
	 * // latencyColumns=7 latencyShift=2
	 * <=1ms >1ms >4ms >16ms >64ms >256ms >1024ms
	 * }</pre>
	 * Default: 3
	 */
	public int latencyShift = 3;

	/**
	 * Copy constructor.
	 */
	public MetricsPolicy(MetricsPolicy other) {
		this.listener = other.listener;
		this.reportDir = other.reportDir;
		this.reportSizeLimit = other.reportSizeLimit;
		this.interval = other.interval;
		this.latencyColumns = other.latencyColumns;
		this.latencyShift = other.latencyShift;
	}

	/**
	 * Default constructor.
	 */
	public MetricsPolicy() {
	}
}
