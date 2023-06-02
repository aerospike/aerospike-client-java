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
package com.aerospike.client.policy;

/**
 * Client metrics configuration.
 */
public final class MetricsPolicy {
	/**
	 * File path to append cluster metrics and latency histograms.
	 */
	public String reportPath;

	/**
	 * Number of cluster tend iterations between metrics log messages. One tend iteration is defined as
	 * {@link ClientPolicy#tendInterval} (default 1 second) plus the time to tend all nodes.
	 * <p>
	 * Default: 30
	 */
	public int reportInterval = 30;

	/**
	 * Number of elapsed time range buckets in latency histograms.
	 * <p>
	 * Default: 5
	 */
	public int latencyColumns = 5;

	/**
	 * Power of 2 multiple between each range bucket in latency histograms starting at column 3. The bucket units
	 * are in milliseconds. The first 2 buckets are "<=1ms" and ">1ms".
	 * <p>
	 * For example, latencyColumns=5 and latencyShift=3 produces the following histogram buckets:
	 * <pre>{@code
	 * <=1ms >1ms >8ms >64ms >512ms
	 * }</pre>
	 * <p>
	 * Default: 3
	 */
	public int latencyShift = 3;

	/**
	 * Copy constructor.
	 */
	public MetricsPolicy(MetricsPolicy other) {
		this.reportPath = other.reportPath;
		this.reportInterval = other.reportInterval;
		this.latencyColumns = other.latencyColumns;
		this.latencyShift = other.latencyShift;
	}

	/**
	 * Default constructor.
	 */
	public MetricsPolicy() {
	}
}
