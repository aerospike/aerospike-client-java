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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.aerospike.client.configuration.primitiveprops.BooleanProperty;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.DynamicConfiguration;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicMetricsConfig;
import com.aerospike.client.metrics.IMetricsExporter;
import com.aerospike.client.metrics.MetricsPolicy;
import com.aerospike.client.metrics.MetricsSnapshot;

public class TestMetricsPolicy {

	@Test
	public void testDefaultValues() {
		MetricsPolicy policy = new MetricsPolicy();

		assertTrue(policy.enableExtendedMetrics);
		assertEquals(30, policy.interval);
		assertEquals(7, policy.latencyColumns);
		assertEquals(1, policy.latencyShift);
		assertEquals(3, policy.maxConsecutiveFailures);
		assertEquals(60, policy.suspendRetryInterval);
		assertEquals(10, policy.exportTimeout);
		assertTrue(policy.getExporters().isEmpty());
		assertFalse(policy.isMetricsRestartRequired());
	}

	@Test
	public void testCopyConstructor() {
		MetricsPolicy original = new MetricsPolicy();
		original.enableExtendedMetrics = false;
		original.interval = 10;
		original.latencyColumns = 5;
		original.latencyShift = 3;
		original.maxConsecutiveFailures = 5;
		original.suspendRetryInterval = 120;
		original.exportTimeout = 15;

		IMetricsExporter exporter = new NoOpExporter();
		original.addExporter(exporter);

		MetricsPolicy copy = new MetricsPolicy(original);

		assertFalse(copy.enableExtendedMetrics);
		assertEquals(10, copy.interval);
		assertEquals(5, copy.latencyColumns);
		assertEquals(3, copy.latencyShift);
		assertEquals(5, copy.maxConsecutiveFailures);
		assertEquals(120, copy.suspendRetryInterval);
		assertEquals(15, copy.exportTimeout);
		assertEquals(1, copy.getExporters().size());
		assertEquals(exporter, copy.getExporters().get(0));
	}

	@Test
	public void testAddExporter() {
		MetricsPolicy policy = new MetricsPolicy();
		IMetricsExporter first = new NoOpExporter();
		IMetricsExporter second = new NoOpExporter();

		policy.addExporter(first);
		policy.addExporter(second);

		List<IMetricsExporter> exporters = policy.getExporters();
		assertEquals(2, exporters.size());
		assertEquals(first, exporters.get(0));
		assertEquals(second, exporters.get(1));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetExportersUnmodifiable() {
		MetricsPolicy policy = new MetricsPolicy();
		policy.addExporter(new NoOpExporter());

		policy.getExporters().add(new NoOpExporter());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddNullExporterThrows() {
		MetricsPolicy policy = new MetricsPolicy();
		policy.addExporter(null);
	}

	@Test
	public void testConfigMergeEnableExtendedMetrics() {
		MetricsPolicy original = new MetricsPolicy();
		assertTrue(original.enableExtendedMetrics);

		Configuration config = buildConfigWithExtendedMetrics(false);

		MetricsPolicy merged = new MetricsPolicy(original, config, true);

		assertFalse(merged.enableExtendedMetrics);
		assertTrue(merged.isMetricsRestartRequired());
	}

	@Test
	public void testConfigMergeNoChange() {
		MetricsPolicy original = new MetricsPolicy();
		assertTrue(original.enableExtendedMetrics);

		Configuration config = buildConfigWithExtendedMetrics(true);

		MetricsPolicy merged = new MetricsPolicy(original, config, true);

		assertTrue(merged.enableExtendedMetrics);
		assertFalse(merged.isMetricsRestartRequired());
	}

	@Test
	public void testConfigMergeNullConfig() {
		MetricsPolicy original = new MetricsPolicy();
		original.enableExtendedMetrics = false;
		original.interval = 15;

		MetricsPolicy merged = new MetricsPolicy(original, null, true);

		assertFalse(merged.enableExtendedMetrics);
		assertEquals(15, merged.interval);
		assertFalse(merged.isMetricsRestartRequired());
	}

	@Test
	public void testConfigMergeNullDynamicMetricsConfig() {
		MetricsPolicy original = new MetricsPolicy();

		DynamicConfiguration dynConfig = new DynamicConfiguration();
		dynConfig.dynamicMetricsConfig = null;

		Configuration config = new Configuration();
		config.dynamicConfiguration = dynConfig;

		MetricsPolicy merged = new MetricsPolicy(original, config, true);

		assertTrue(merged.enableExtendedMetrics);
		assertFalse(merged.isMetricsRestartRequired());
	}

	@Test
	public void testConfigMergeLatencyColumnsValidation() {
		MetricsPolicy original = new MetricsPolicy();
		original.latencyColumns = 7;

		DynamicMetricsConfig dynMC = new DynamicMetricsConfig();
		dynMC.latencyColumns = new com.aerospike.client.configuration.primitiveprops.IntProperty();
		dynMC.latencyColumns.value = 0;

		DynamicConfiguration dynConfig = new DynamicConfiguration();
		dynConfig.dynamicMetricsConfig = dynMC;

		Configuration config = new Configuration();
		config.dynamicConfiguration = dynConfig;

		MetricsPolicy merged = new MetricsPolicy(original, config, true);

		assertEquals(7, merged.latencyColumns);
	}

	/**
	 * Build a minimal Configuration with only the enableExtendedMetrics field set.
	 */
	private Configuration buildConfigWithExtendedMetrics(boolean value) {
		DynamicMetricsConfig dynMC = new DynamicMetricsConfig();
		dynMC.enableExtendedMetrics = new BooleanProperty(value);

		DynamicConfiguration dynConfig = new DynamicConfiguration();
		dynConfig.dynamicMetricsConfig = dynMC;

		Configuration config = new Configuration();
		config.dynamicConfiguration = dynConfig;
		return config;
	}

	@Test
	public void testDynamicMetricsConfigGetterSetter() {
		DynamicMetricsConfig dynMC = new DynamicMetricsConfig();

		dynMC.setEnableExtendedMetrics(new BooleanProperty(false));
		assertFalse(dynMC.getEnableExtendedMetrics().value);

		dynMC.setEnableExtendedMetrics(new BooleanProperty(true));
		assertTrue(dynMC.getEnableExtendedMetrics().value);
	}

	@Test
	public void testDynamicMetricsConfigToStringNullSafe() {
		DynamicMetricsConfig dynMC = new DynamicMetricsConfig();
		dynMC.enable = new BooleanProperty(true);
		dynMC.enableExtendedMetrics = null;

		String result = dynMC.toString();
		assertTrue(result.contains("enable_extended_metrics=null"));
	}

	/**
	 * Minimal no-op IMetricsExporter for testing.
	 */
	private static class NoOpExporter implements IMetricsExporter {
		@Override
		public void export(MetricsSnapshot snapshot) {
		}

		@Override
		public void close() {
		}
	}
}
