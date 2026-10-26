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

import java.time.Duration;
import java.util.List;

import org.junit.Test;

import com.aerospike.client.configuration.primitiveprops.BooleanProperty;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.DynamicConfiguration;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicMetricsConfig;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicMetricsExtendedConfig;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicMetricsOperationalConfig;
import com.aerospike.client.metrics.IMetricsExporter;
import com.aerospike.client.metrics.MetricsExporterType;
import com.aerospike.client.metrics.MetricsPolicy;
import com.aerospike.client.metrics.MetricsSnapshot;

public class TestMetricsPolicy {

	@Test
	public void testDefaultValues() {
		MetricsPolicy policy = new MetricsPolicy();

		assertTrue(policy.enableOperationalMetrics);
		assertEquals(30, policy.interval);
		assertEquals(Duration.ofSeconds(30), policy.exportInterval);
		assertEquals(MetricsExporterType.FILE, policy.exporter);
		assertEquals(7, policy.latencyColumns);
		assertEquals(1, policy.latencyShift);
		assertEquals(3, policy.maxConsecutiveFailures);
		assertEquals(60, policy.suspendRetryInterval);
		assertTrue(policy.getExporters().isEmpty());
	}

	@Test
	public void testCopyConstructor() {
		MetricsPolicy original = new MetricsPolicy();
		original.enableOperationalMetrics = false;
		original.interval = 10;
		original.latencyColumns = 5;
		original.latencyShift = 3;
		original.maxConsecutiveFailures = 5;
		original.suspendRetryInterval = 120;
		original.exportInterval = Duration.ofSeconds(15);

		IMetricsExporter exporter = new NoOpExporter();
		original.addExporter(exporter);

		MetricsPolicy copy = new MetricsPolicy(original);

		assertFalse(copy.enableOperationalMetrics);
		assertEquals(10, copy.interval);
		assertEquals(5, copy.latencyColumns);
		assertEquals(3, copy.latencyShift);
		assertEquals(5, copy.maxConsecutiveFailures);
		assertEquals(120, copy.suspendRetryInterval);
		assertEquals(Duration.ofSeconds(15), copy.exportInterval);
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
	public void testConfigMergeOperationalMetrics() {
		MetricsPolicy original = new MetricsPolicy();
		assertTrue(original.enableOperationalMetrics);

		Configuration config = buildConfigWithOperationalMetrics(false);

		MetricsPolicy merged = new MetricsPolicy(original, config);

		assertFalse(merged.enableOperationalMetrics);
	}

	@Test
	public void testConfigMergeNoChange() {
		MetricsPolicy original = new MetricsPolicy();
		assertTrue(original.enableOperationalMetrics);

		Configuration config = buildConfigWithOperationalMetrics(true);

		MetricsPolicy merged = new MetricsPolicy(original, config);

		assertTrue(merged.enableOperationalMetrics);
	}

	@Test
	public void testConfigMergeNullConfig() {
		MetricsPolicy original = new MetricsPolicy();
		original.enableOperationalMetrics = false;
		original.interval = 15;

		MetricsPolicy merged = new MetricsPolicy(original, null);

		assertFalse(merged.enableOperationalMetrics);
		assertEquals(15, merged.interval);
	}

	@Test
	public void testConfigMergeNullDynamicMetricsConfig() {
		MetricsPolicy original = new MetricsPolicy();

		DynamicConfiguration dynConfig = new DynamicConfiguration();
		dynConfig.dynamicMetricsConfig = null;

		Configuration config = new Configuration();
		config.dynamicConfiguration = dynConfig;

		MetricsPolicy merged = new MetricsPolicy(original, config);

		assertTrue(merged.enableOperationalMetrics);
	}

	@Test
	public void testConfigMergeRetainsInvalidLatencyColumnsForValidation() {
		MetricsPolicy original = new MetricsPolicy();
		original.latencyColumns = 7;

		DynamicMetricsConfig dynMC = new DynamicMetricsConfig();
		dynMC.latencyColumns = new com.aerospike.client.configuration.primitiveprops.IntProperty();
		dynMC.latencyColumns.value = 0;

		DynamicConfiguration dynConfig = new DynamicConfiguration();
		dynConfig.dynamicMetricsConfig = dynMC;

		Configuration config = new Configuration();
		config.dynamicConfiguration = dynConfig;

		MetricsPolicy merged = new MetricsPolicy(original, config);

		assertEquals(0, merged.latencyColumns);

		try {
			merged.validate();
		}
		catch (IllegalArgumentException expected) {
			return;
		}
		throw new AssertionError("Expected invalid latency columns to be rejected");
	}

	@Test
	public void testConfigMergeNestedOperationalHistogramSettings() {
		MetricsPolicy original = new MetricsPolicy();
		DynamicMetricsConfig dynMC = new DynamicMetricsConfig();
		DynamicMetricsExtendedConfig extended = new DynamicMetricsExtendedConfig();
		DynamicMetricsOperationalConfig operational = new DynamicMetricsOperationalConfig();
		operational.latencyColumns =
			new com.aerospike.client.configuration.primitiveprops.IntProperty(9);
		operational.latencyShift =
			new com.aerospike.client.configuration.primitiveprops.IntProperty(3);
		extended.operational = operational;
		dynMC.extended = extended;

		DynamicConfiguration dynConfig = new DynamicConfiguration();
		dynConfig.dynamicMetricsConfig = dynMC;
		Configuration config = new Configuration();
		config.dynamicConfiguration = dynConfig;

		MetricsPolicy merged = new MetricsPolicy(original, config);

		assertEquals(9, merged.latencyColumns);
		assertEquals(3, merged.latencyShift);
	}

	private static Configuration buildConfigWithOperationalMetrics(boolean value) {
		DynamicMetricsConfig dynMC = new DynamicMetricsConfig();
		DynamicMetricsExtendedConfig extended = new DynamicMetricsExtendedConfig();
		DynamicMetricsOperationalConfig operational = new DynamicMetricsOperationalConfig();
		operational.enabled = new BooleanProperty(value);
		extended.operational = operational;
		dynMC.extended = extended;

		DynamicConfiguration dynConfig = new DynamicConfiguration();
		dynConfig.dynamicMetricsConfig = dynMC;

		Configuration config = new Configuration();
		config.dynamicConfiguration = dynConfig;
		return config;
	}

	@Test
	public void testValidationAcceptsBoundaryValues() {
		MetricsPolicy policy = new MetricsPolicy();
		policy.interval = 1;
		policy.maxConsecutiveFailures = 1;
		policy.suspendRetryInterval = 0;
		policy.exportInterval = Duration.ofMillis(1);

		policy.validate();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidationRejectsNonPositiveInterval() {
		MetricsPolicy policy = new MetricsPolicy();
		policy.interval = 0;
		policy.validate();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidationRejectsNonPositiveFailureLimit() {
		MetricsPolicy policy = new MetricsPolicy();
		policy.maxConsecutiveFailures = 0;
		policy.validate();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidationRejectsNegativeRetryInterval() {
		MetricsPolicy policy = new MetricsPolicy();
		policy.suspendRetryInterval = -1;
		policy.validate();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidationRejectsNonPositiveExportInterval() {
		MetricsPolicy policy = new MetricsPolicy();
		policy.exportInterval = Duration.ZERO;
		policy.validate();
	}

	private static class NoOpExporter implements IMetricsExporter {
		@Override
		public void export(MetricsSnapshot snapshot) {
		}

	}
}
