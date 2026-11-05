/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.configuration.serializers.dynamicconfig;

import com.aerospike.client.configuration.primitiveprops.BooleanProperty;
import com.aerospike.client.configuration.primitiveprops.IntProperty;
import com.aerospike.client.configuration.primitiveprops.StringProperty;

/**
 * YAML model for operational metrics and latency histogram settings.
 */
public class DynamicMetricsOperationalConfig {
	/** Whether operational metrics collection is enabled. */
	public BooleanProperty enabled;

	/** Unit used by latency histogram snapshots. */
	public StringProperty latencyUnit;

	/** Number of latency histogram columns. */
	public IntProperty latencyColumns;

	/** Power-of-two shift between latency histogram columns. */
	public IntProperty latencyShift;

	/** Operational metrics sampling settings. */
	public DynamicMetricsSamplerConfig sampler;

	/** Return whether operational metrics collection is enabled. */
	public BooleanProperty getEnabled() {
		return enabled;
	}

	/** Set whether operational metrics collection is enabled. */
	public void setEnabled(BooleanProperty enabled) {
		this.enabled = enabled;
	}

	/** Return the latency histogram unit. */
	public StringProperty getLatencyUnit() {
		return latencyUnit;
	}

	/** Set the latency histogram unit. */
	public void setLatencyUnit(StringProperty latencyUnit) {
		this.latencyUnit = latencyUnit;
	}

	/** Return the latency histogram column count. */
	public IntProperty getLatencyColumns() {
		return latencyColumns;
	}

	/** Set the latency histogram column count. */
	public void setLatencyColumns(IntProperty latencyColumns) {
		this.latencyColumns = latencyColumns;
	}

	/** Return the latency histogram shift. */
	public IntProperty getLatencyShift() {
		return latencyShift;
	}

	/** Set the latency histogram shift. */
	public void setLatencyShift(IntProperty latencyShift) {
		this.latencyShift = latencyShift;
	}

	/** Return operational metrics sampling settings. */
	public DynamicMetricsSamplerConfig getSampler() {
		return sampler;
	}

	/** Set operational metrics sampling settings. */
	public void setSampler(DynamicMetricsSamplerConfig sampler) {
		this.sampler = sampler;
	}
}
