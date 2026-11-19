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
package com.aerospike.client.cluster;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.function.Consumer;

import org.junit.Test;

import com.aerospike.client.metrics.LatencyUnit;
import com.aerospike.client.metrics.MetricsExporterType;
import com.aerospike.client.metrics.MetricsPolicy;

public class TestMetricsRuntimeSettings {
	@Test
	public void testEquivalentPolicyDoesNotRequireRestart() {
		MetricsPolicy previous = new MetricsPolicy();
		MetricsPolicy current = new MetricsPolicy(previous);

		assertFalse(Cluster.metricsRuntimeSettingsChanged(previous, current));
	}

	@Test
	public void testEveryRuntimeSettingRequiresRestart() {
		assertRestart(policy -> policy.exportInterval = Duration.ofSeconds(1));
		assertRestart(policy -> policy.exporter = MetricsExporterType.NONE);
		assertRestart(policy -> policy.reportDir = "metrics");
		assertRestart(policy -> policy.reportSizeLimit = 1000000);
		assertRestart(policy ->
			policy.labels = Collections.singletonMap("environment", "test"));
		assertRestart(policy -> policy.enableOperationalMetrics = false);
		assertRestart(policy -> policy.enableUsageMetrics = true);
		assertRestart(policy -> policy.latencyUnit = LatencyUnit.MICROSECONDS);
		assertRestart(policy -> policy.latencyColumns = 8);
		assertRestart(policy -> policy.latencyShift = 2);
		assertRestart(policy -> policy.samplerRange = 10);
		assertRestart(policy -> policy.samplerThreshold = 0);
	}

	private static void assertRestart(Consumer<MetricsPolicy> update) {
		MetricsPolicy previous = new MetricsPolicy();
		MetricsPolicy current = new MetricsPolicy(previous);
		update.accept(current);

		assertTrue(Cluster.metricsRuntimeSettingsChanged(previous, current));
	}
}
