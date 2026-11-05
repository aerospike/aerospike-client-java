/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.Log;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicMetricsConfig;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicMetricsOperationalConfig;
import com.aerospike.client.policy.ClientPolicy;

/**
 * Client periodic metrics configuration.
 */
public final class MetricsPolicy {
	/**
	 * Legacy listener that handles metrics notification events.
	 * <p>
	 * New integrations should use {@link IMetricsExporter}. This field is retained for
	 * compatibility with existing custom listeners.
	 */
	public MetricsListener listener;

	/**
	 * Directory path used by the FILE metrics exporter.
	 * <p>
	 * Default: <current directory>
	 */
	public String reportDir = ".";

	/**
	 * Metrics file size soft limit in bytes for the FILE metrics exporter.
	 * <p>
	 * When reportSizeLimit is reached or exceeded, the current metrics file is closed and a new
	 * metrics file is created with a new timestamp. If reportSizeLimit is zero, the metrics file
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
	 * Must be greater than zero when exporters are configured.
	 * <p>
	 * Default: 30
	 */
	public int interval = 30;

	/**
	 * Elapsed time between snapshot exports.
	 * <p>
	 * Default: 30 seconds
	 */
	public Duration exportInterval = Duration.ofSeconds(30);

	/**
	 * Periodic snapshot export destination.
	 * <p>
	 * Default: FILE
	 */
	public MetricsExporterType exporter = MetricsExporterType.FILE;

	/**
	 * Number of elapsed time range buckets in latency histograms.
	 * <p>
	 * Default: 7
	 */
	public int latencyColumns = 7;

	/**
	 * Power of 2 multiple between each range bucket in latency histograms starting at column 3. The bucket units
	 * are in milliseconds. The first 2 buckets are "&lt;=1ms" and "&gt;1ms". Examples:
	 * <pre>{@code
	 * // latencyColumns=7 latencyShift=1
	 * <=1ms >1ms >2ms >4ms >8ms >16ms >32ms
	 *
	 * // latencyColumns=5 latencyShift=3
	 * <=1ms >1ms >8ms >64ms >512ms
	 * }</pre>
	 * Default: 1
	 */
	public int latencyShift = 1;

	/**
	 * Enable collection of operational metrics such as CPU usage, memory usage, command count,
	 * per-namespace error/timeout/byte counters, and latency histograms. Operational metrics
	 * provide detailed diagnostics but may add overhead on the command hot path.
	 * <p>
	 * When {@code false}, only standard low-overhead metrics (connection pool gauges, retry
	 * counts, cluster health counters) are collected and exported.
	 * <p>
	 * Default: true (all metrics collected when metrics are enabled)
	 */
	public boolean enableOperationalMetrics = true;

	/**
	 * Enable usage metrics in exported snapshots.
	 * Collection of these metrics is implemented separately.
	 */
	public boolean enableUsageMetrics;

	/**
	 * Unit used by latency snapshots.
	 */
	public LatencyUnit latencyUnit = LatencyUnit.MILLISECONDS;

	/**
	 * Operational metrics sampler range. Collection behavior is implemented separately.
	 */
	// TODO: Apply these settings when operational command-path sampling is implemented.
	public int samplerRange = 1;

	/**
	 * Operational metrics sampler threshold. Collection behavior is implemented separately.
	 */
	public int samplerThreshold = 1;

	/**
	 * Labels that can be sent to the metrics output
	 */
	public Map<String,String> labels;

	/**
	 * Registered metrics exporters. Invoked in registration order by the
	 * dedicated metrics thread. Registrations are captured when metrics are
	 * enabled and remain fixed until metrics are disabled and enabled again.
	 * Custom registrations are used when {@link #exporter} is
	 * {@link MetricsExporterType#CUSTOM}.
	 */
	private final List<IMetricsExporter> exporters = new ArrayList<>();

	/**
	 * Maximum consecutive export failures before an exporter is suspended.
	 * Must be greater than zero.
	 * <p>
	 * Default: 3
	 */
	public int maxConsecutiveFailures = 3;

	/**
	 * Seconds to wait before retrying a suspended exporter.
	 * Zero retries the exporter on the next snapshot interval.
	 * Must not be negative.
	 * <p>
	 * Default: 60
	 */
	public int suspendRetryInterval = 60;

	/**
	 * Copy a metrics policy and apply configured overrides.
	 */
	public MetricsPolicy(MetricsPolicy other, Configuration config) {
		this(other);
		boolean logUpdate = false;
		if (config == null) {
			return;
		}
		DynamicMetricsConfig dynMC = config.dynamicConfiguration.dynamicMetricsConfig;
		if (dynMC == null) {
			return;
		}
		if (Log.infoEnabled()) {
			logUpdate = true;
		}
		if (dynMC.labels != null && !dynMC.labels.equals(this.labels)) {
			this.labels = new HashMap<>(dynMC.labels);
			if (logUpdate) {
				Log.info("Set MetricsPolicy.labels = " + this.labels);
			}
		}
		if (dynMC.latencyShift != null)  {
			if (dynMC.latencyShift.value != this.latencyShift) {
				this.latencyShift = dynMC.latencyShift.value;
				if (logUpdate) {
					Log.info("Set MetricsPolicy.latencyShift = " + this.latencyShift);
				}
			}
		}
		if (dynMC.latencyColumns != null) {
			if (dynMC.latencyColumns.value != this.latencyColumns) {
				this.latencyColumns = dynMC.latencyColumns.value;
				if (logUpdate) {
					Log.info("Set MetricsPolicy.latencyColumns = " + this.latencyColumns);
				}
			}
		}
		if (dynMC.exportInterval != null) {
			this.exportInterval = dynMC.exportInterval.value;
		}
		if (dynMC.exporter != null) {
			this.exporter = MetricsExporterType.parse(dynMC.exporter.value);
		}
		if (dynMC.reportDir != null) {
			this.reportDir = dynMC.reportDir.value;
		}
		if (dynMC.reportSizeLimit != null) {
			this.reportSizeLimit = dynMC.reportSizeLimit.value;
		}

		DynamicMetricsOperationalConfig operational = dynMC.extended == null ? null :
			dynMC.extended.operational;

		if (operational != null) {
			if (operational.enabled != null) {
				this.enableOperationalMetrics = operational.enabled.value;
			}
			if (operational.latencyUnit != null) {
				this.latencyUnit = LatencyUnit.parse(operational.latencyUnit.value);
			}
			if (operational.latencyColumns != null) {
				if (dynMC.latencyColumns != null &&
					operational.latencyColumns.value != dynMC.latencyColumns.value) {
					throw new IllegalArgumentException(
						"Conflicting metrics.extended.operational.latency_columns and metrics.latency_columns");
				}
				this.latencyColumns = operational.latencyColumns.value;
			}
			if (operational.latencyShift != null) {
				if (dynMC.latencyShift != null &&
					operational.latencyShift.value != dynMC.latencyShift.value) {
					throw new IllegalArgumentException(
						"Conflicting metrics.extended.operational.latency_shift and metrics.latency_shift");
				}
				this.latencyShift = operational.latencyShift.value;
			}
			if (operational.sampler != null) {
				if (operational.sampler.range != null) {
					this.samplerRange = operational.sampler.range.value;
				}
				if (operational.sampler.threshold != null) {
					this.samplerThreshold = operational.sampler.threshold.value;
				}
			}
		}
		if (dynMC.extended != null && dynMC.extended.usage != null &&
			dynMC.extended.usage.enabled != null) {
			this.enableUsageMetrics = dynMC.extended.usage.enabled.value;
		}
	}

	/**
	 * Copy constructor.
	 */
	public MetricsPolicy(MetricsPolicy other) {
		this.listener = other.listener;
		this.reportDir = other.reportDir;
		this.reportSizeLimit = other.reportSizeLimit;
		this.interval = other.interval;
		this.exportInterval = other.exportInterval;
		this.exporter = other.exporter;
		this.latencyColumns = other.latencyColumns;
		this.latencyShift = other.latencyShift;
		this.enableOperationalMetrics = other.enableOperationalMetrics;
		this.enableUsageMetrics = other.enableUsageMetrics;
		this.latencyUnit = other.latencyUnit;
		this.samplerRange = other.samplerRange;
		this.samplerThreshold = other.samplerThreshold;
		this.labels = other.labels == null ? null : new HashMap<>(other.labels);
		this.exporters.addAll(other.exporters);
		this.maxConsecutiveFailures = other.maxConsecutiveFailures;
		this.suspendRetryInterval = other.suspendRetryInterval;
	}

	/**
	 * Default constructor.
	 */
	public MetricsPolicy() {
	}

	// Include setters to facilitate Spring's ConfigurationProperties.

	/** Set the legacy metrics listener. */
	public void setListener(MetricsListener listener) {
		this.listener = listener;
	}

	/** Set the FILE exporter directory. */
	public void setReportDir(String reportDir) {
		this.reportDir = reportDir;
	}

	/** Set the FILE exporter rotation limit in bytes. */
	public void setReportSizeLimit(long reportSizeLimit) {
		this.reportSizeLimit = reportSizeLimit;
	}

	/** Set the legacy tend-iteration notification interval. */
	public void setInterval(int interval) {
		this.interval = interval;
	}

	/** Set the elapsed time between snapshot exports. */
	public void setExportInterval(Duration exportInterval) {
		this.exportInterval = exportInterval;
	}

	/** Return the elapsed time between snapshot exports. */
	public Duration getExportInterval() {
		return exportInterval;
	}

	/** Set the periodic snapshot export destination. */
	public void setExporter(MetricsExporterType exporter) {
		this.exporter = exporter;
	}

	/** Set the latency histogram column count. */
	public void setLatencyColumns(int latencyColumns) {
		this.latencyColumns = latencyColumns;
	}

	/** Set the latency histogram column shift. */
	public void setLatencyShift(int latencyShift) { this.latencyShift = latencyShift; }

	/** Set whether operational metrics collection is enabled. */
	public void setEnableOperationalMetrics(boolean enableOperationalMetrics) {
		this.enableOperationalMetrics = enableOperationalMetrics;
	}

	/** Set whether usage metrics collection is enabled. */
	public void setEnableUsageMetrics(boolean enableUsageMetrics) {
		this.enableUsageMetrics = enableUsageMetrics;
	}

	/** Set labels attached to exported snapshots. */
	public void setLabels(Map<String, String> labels) { this.labels = labels; }

	/**
	 * Register a metrics exporter. Exporters are invoked in registration order.
	 * If metrics are already enabled, the exporter is used the next time metrics
	 * are disabled and enabled.
	 *
	 * @param exporter the exporter to add; must not be null
	 * @throws IllegalArgumentException if exporter is null
	 */
	public void addExporter(IMetricsExporter exporter) {
		if (exporter == null) {
			throw new IllegalArgumentException("exporter must not be null");
		}
		exporters.add(exporter);
		this.exporter = MetricsExporterType.CUSTOM;
	}

	/**
	 * Return an unmodifiable view of the registered exporters.
	 */
	public List<IMetricsExporter> getExporters() {
		return Collections.unmodifiableList(exporters);
	}

	/**
	 * Validate settings used by metrics collection and export.
	 *
	 * @throws IllegalArgumentException if a metrics setting is outside its
	 * supported range
	 */
	public void validate() {
		if (interval <= 0) {
			throw new IllegalArgumentException("MetricsPolicy.interval must be greater than zero");
		}
		if (maxConsecutiveFailures <= 0) {
			throw new IllegalArgumentException(
				"MetricsPolicy.maxConsecutiveFailures must be greater than zero");
		}
		if (suspendRetryInterval < 0) {
			throw new IllegalArgumentException(
				"MetricsPolicy.suspendRetryInterval must not be negative");
		}
		if (exportInterval == null || exportInterval.isZero() || exportInterval.isNegative()) {
			throw new IllegalArgumentException(
				"MetricsPolicy.exportInterval must be greater than zero");
		}
		if (reportSizeLimit != 0 && reportSizeLimit < 1000000) {
			throw new IllegalArgumentException(
				"MetricsPolicy.reportSizeLimit must be zero or at least 1000000");
		}
		if (latencyColumns <= 0) {
			throw new IllegalArgumentException(
				"MetricsPolicy.latencyColumns must be greater than zero");
		}
		if (latencyShift < 0 || latencyShift > 30) {
			throw new IllegalArgumentException(
				"MetricsPolicy.latencyShift must be between 0 and 30");
		}
		if (samplerRange <= 0 || samplerThreshold < 0 || samplerThreshold > samplerRange) {
			throw new IllegalArgumentException(
				"MetricsPolicy sampler requires range > 0 and threshold between 0 and range");
		}
	}

	/** Set the consecutive failure threshold for exporter suspension. */
	public void setMaxConsecutiveFailures(int maxConsecutiveFailures) {
		this.maxConsecutiveFailures = maxConsecutiveFailures;
	}

	/** Set the suspended-exporter retry interval in seconds. */
	public void setSuspendRetryInterval(int suspendRetryInterval) {
		this.suspendRetryInterval = suspendRetryInterval;
	}

}
