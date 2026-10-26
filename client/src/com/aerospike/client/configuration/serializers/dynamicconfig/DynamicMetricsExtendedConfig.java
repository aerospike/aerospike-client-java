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

/**
 * YAML model containing operational and usage metrics settings.
 */
public class DynamicMetricsExtendedConfig {
	/** Operational metrics settings. */
	public DynamicMetricsOperationalConfig operational;

	/** Usage metrics settings. */
	public DynamicMetricsUsageConfig usage;

	/** Return operational metrics settings. */
	public DynamicMetricsOperationalConfig getOperational() {
		return operational;
	}

	/** Set operational metrics settings. */
	public void setOperational(DynamicMetricsOperationalConfig operational) {
		this.operational = operational;
	}

	/** Return usage metrics settings. */
	public DynamicMetricsUsageConfig getUsage() {
		return usage;
	}

	/** Set usage metrics settings. */
	public void setUsage(DynamicMetricsUsageConfig usage) {
		this.usage = usage;
	}
}
