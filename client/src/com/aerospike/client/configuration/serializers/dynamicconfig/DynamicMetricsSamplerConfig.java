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

import com.aerospike.client.configuration.primitiveprops.IntProperty;

/**
 * YAML model for operational metrics sampling.
 */
public class DynamicMetricsSamplerConfig {
	/** Sampling range used as the threshold denominator. */
	public IntProperty range;

	/** Sampling threshold within the configured range. */
	public IntProperty threshold;

	/** Return the sampling range. */
	public IntProperty getRange() {
		return range;
	}

	/** Set the sampling range. */
	public void setRange(IntProperty range) {
		this.range = range;
	}

	/** Return the sampling threshold. */
	public IntProperty getThreshold() {
		return threshold;
	}

	/** Set the sampling threshold. */
	public void setThreshold(IntProperty threshold) {
		this.threshold = threshold;
	}
}
