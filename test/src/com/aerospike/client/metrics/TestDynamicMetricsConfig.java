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
package com.aerospike.client.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.junit.Test;

import com.aerospike.client.configuration.YamlConfigProvider;
import com.aerospike.client.configuration.serializers.Configuration;

public class TestDynamicMetricsConfig {
	@Test
	public void testCompleteV3MetricsTree() {
		String path = "file://" + System.getProperty("user.dir") +
			"/src/resources/aerospikeconfig_v3_metrics.yaml";
		Configuration config = new YamlConfigProvider(path).fetchConfiguration();
		MetricsPolicy policy = new MetricsPolicy(new MetricsPolicy(), config);

		assertEquals(Duration.ofSeconds(45), policy.exportInterval);
		assertEquals(MetricsExporterType.CUSTOM, policy.exporter);
		assertEquals("metrics", policy.reportDir);
		assertEquals(2L * 1024L * 1024L, policy.reportSizeLimit);
		assertEquals(LatencyUnit.MICROSECONDS, policy.latencyUnit);
		assertEquals(8, policy.latencyColumns);
		assertEquals(2, policy.latencyShift);
		assertEquals(10, policy.samplerRange);
		assertEquals(3, policy.samplerThreshold);
		assertTrue(policy.enableOperationalMetrics);
		assertTrue(policy.enableUsageMetrics);
	}
}
