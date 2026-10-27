/*
 * Copyright (c) 2012-2026 Aerospike, Inc.
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
package com.aerospike.client.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.Endpoint.SourceList;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.SeedCandidateContext;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.discovery.StaticMapAddressTranslator;
import com.aerospike.client.discovery.StaticSeedCandidateProvider;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;

public class YamlDiscoveryConfigTest {
	private static final String CONFIG_SYS_PROP = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";

	@After
	public void reset() {
		System.clearProperty(CONFIG_SYS_PROP);
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private static String path(String name) throws Exception {
		return Paths.get(YamlDiscoveryConfigTest.class.getResource("/yaml-config/" + name + ".yaml").toURI()).toString();
	}

	private static ClientPolicy load(String name) throws Exception {
		return load(new ClientPolicy(), name);
	}

	private static ClientPolicy load(ClientPolicy policy, String name) throws Exception {
		YamlConfigProvider provider = YamlConfigProvider.getConfigProvider(path(name));
		assertNotNull(provider);
		return new ClientPolicy(policy, provider);
	}

	private static Host translate(ClientPolicy policy, String nodeName, String host, int port) {
		Optional<Host> result = policy.addressTranslator.translate(
			new Endpoint(nodeName, null, host, port, SourceList.STANDARD));
		return result.orElse(null);
	}

	@Test
	public void everyKeySetsItsField() throws Exception {
		ClientPolicy policy = load("all");

		assertEquals(99, policy.maxConnsPerNode);
		assertEquals(Map.of("10.0.0.1", "192.168.1.1", "10.0.0.2", "192.168.1.2"), policy.ipMap);
		assertTrue(policy.useServicesAlternate);
		assertNull(policy.seedCandidateProvider);
		assertEquals(SeedMergePolicy.REPLACE, policy.discoveryMergePolicy);
		assertEquals(60000, policy.discoveryRefreshInterval);
		assertEquals(200, policy.discoveryTendRefreshDeadline);
		assertNull(policy.addressTranslator);
		assertEquals(TranslationMissMode.STRICT, policy.translationOnMiss);
	}

	@Test
	public void staticMapBuildsStaticMapTranslator() throws Exception {
		for (String name : List.of("static-map", "static-map-implied")) {
			ClientPolicy policy = load(name);

			assertTrue(name, policy.addressTranslator instanceof StaticMapAddressTranslator);
			assertNull(policy.ipMap);
			Host host = translate(policy, "BB9", "10.0.0.1", 3000);
			assertEquals("192.168.1.1", host.name);
			assertEquals(3000, host.port);
			assertNull(translate(policy, "BB9", "10.0.0.2", 3000));
		}
	}

	@Test
	public void portMapBuildsPortMapTranslator() throws Exception {
		ClientPolicy policy = load("port-map");

		assertTrue(policy.addressTranslator instanceof PortMapAddressTranslator);
		assertEquals(SeedMergePolicy.MERGE, policy.discoveryMergePolicy);
		assertEquals(new Host("lb.example.com", 30001), translate(policy, "X", "10.0.0.1", 3000));
		assertEquals(new Host("lb.example.com", 30002), translate(policy, "X", "2001:db8::1", 3000));
		assertEquals(new Host("lb.example.com", 30003), translate(policy, "BB9020011AC4202", "10.0.0.9", 3000));
		assertNull(translate(policy, "X", "10.0.0.1", 3001));
	}

	@Test
	public void passThroughBuildsPassThroughTranslator() throws Exception {
		ClientPolicy policy = load("pass-through");

		assertSame(PassThroughAddressTranslator.INSTANCE, policy.addressTranslator);
		assertTrue(policy.useServicesAlternate);
		assertEquals(SeedMergePolicy.DISCOVERY_ONLY, policy.discoveryMergePolicy);
		assertEquals(TranslationMissMode.PASS_THROUGH, policy.translationOnMiss);
	}

	@Test
	public void defaultsApplyWhenBlocksAbsent() throws Exception {
		ClientPolicy defaults = new ClientPolicy();
		ClientPolicy policy = load("no-new-keys");

		assertEquals(99, policy.maxConnsPerNode);
		assertDiscoveryFields(defaults, policy);
	}

	@Test
	public void unknownKeyOutsideNewBlocksIsIgnored() throws Exception {
		YamlConfigProvider provider = YamlConfigProvider.getConfigProvider(path("no-new-keys"));

		assertNotNull(provider);
		assertEquals(99, provider.fetchConfiguration().staticConfiguration.staticClientConfig.maxConnectionsPerNode.value);
		assertEquals(2002, provider.fetchConfiguration().dynamicConfiguration.dynamicClientConfig.timeout.value);
	}

	@Test
	public void absentFileLeavesDefaults() throws Exception {
		String missing = new File(System.getProperty("java.io.tmpdir"), "no-such-aerospike-config.yaml").getPath();

		assertNull(YamlConfigProvider.getConfigProvider(missing));
		assertDiscoveryFields(new ClientPolicy(), new ClientPolicy(new ClientPolicy(), null));
	}

	@Test
	public void translationBlockWithoutProviderKeepsCodeTranslator() throws Exception {
		ClientPolicy code = new ClientPolicy();
		code.addressTranslator = PassThroughAddressTranslator.INSTANCE;

		ClientPolicy policy = load(code, "on-miss-only");

		assertSame(PassThroughAddressTranslator.INSTANCE, policy.addressTranslator);
		assertEquals(TranslationMissMode.STRICT, policy.translationOnMiss);
	}

	@Test
	public void yamlOverridesCode() throws Exception {
		ClientPolicy code = new ClientPolicy();
		code.ipMap = Map.of("10.9.9.9", "1.1.1.1");
		code.seedCandidateProvider = new StaticSeedCandidateProvider(new Host[] {new Host("10.0.0.5", 3000)});
		code.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		code.discoveryMergePolicy = SeedMergePolicy.DISCOVERY_ONLY;
		code.discoveryRefreshInterval = 1000;
		code.discoveryTendRefreshDeadline = 10;

		List<String> messages = new ArrayList<>();
		Log.setLevel(Log.Level.INFO);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.INFO) {
				messages.add(message);
			}
		});

		YamlConfigProvider provider = YamlConfigProvider.getConfigProvider(path("all"));
		ClientPolicy policy = new ClientPolicy(code, provider, true);

		assertEquals(Map.of("10.0.0.1", "192.168.1.1", "10.0.0.2", "192.168.1.2"), policy.ipMap);
		assertNull(policy.seedCandidateProvider);
		assertNull(policy.addressTranslator);
		assertEquals(SeedMergePolicy.REPLACE, policy.discoveryMergePolicy);
		assertEquals(60000, policy.discoveryRefreshInterval);
		assertEquals(200, policy.discoveryTendRefreshDeadline);

		for (String field : List.of("ipMap", "useServicesAlternate", "seedCandidateProvider", "discoveryMergePolicy",
				"discoveryRefreshInterval", "discoveryTendRefreshDeadline", "addressTranslator", "translationOnMiss")) {
			assertTrue(field + " not logged: " + messages,
				messages.stream().anyMatch(m -> m.startsWith("Set ClientPolicy." + field + " = ")));
		}
	}

	@Test
	public void unknownKeysInsideAndOutsideNewBlocksAreIgnored() throws Exception {
		ClientPolicy defaults = new ClientPolicy();
		ClientPolicy policy = load("unknown-keys");

		assertEquals(99, policy.maxConnsPerNode);
		assertEquals(SeedMergePolicy.REPLACE, policy.discoveryMergePolicy);
		assertEquals(defaults.discoveryRefreshInterval, policy.discoveryRefreshInterval);
		assertNull(policy.seedCandidateProvider);
		assertSame(PassThroughAddressTranslator.INSTANCE, policy.addressTranslator);
		assertEquals(defaults.translationOnMiss, policy.translationOnMiss);
	}

	@Test
	public void refreshIntervalBelowTendIntervalFailsConstruction() throws Exception {
		ClientPolicy code = new ClientPolicy();
		code.seedCandidateProvider = new SeedCandidateProvider() {
			@Override
			public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
				return List.of(new Host("127.0.0.1", 1));
			}

			@Override
			public boolean needsPeriodicRefresh() {
				return true;
			}

			@Override
			public boolean supportsDeadline() {
				return true;
			}
		};
		System.setProperty(CONFIG_SYS_PROP, path("refresh-below-tend"));

		try (AerospikeClient client = new AerospikeClient(code, new Host("127.0.0.1", 1))) {
			fail("refresh_interval_ms below tend_interval accepted");
		}
		catch (AerospikeException e) {
			System.out.println("YAML-RANGE " + e.getMessage());
			assertEquals("Discovery refresh interval 500 must be greater or equal to the tend interval 1000", e.getBaseMessage());
		}
	}

	@Test
	public void reloadReappliesChangedKeys() throws Exception {
		Path file = Files.createTempFile("yaml-discovery", ".yaml");

		try {
			Files.copy(Paths.get(path("all")), file, StandardCopyOption.REPLACE_EXISTING);
			YamlConfigProvider provider = YamlConfigProvider.getConfigProvider(file.toString());
			ClientPolicy merged = new ClientPolicy(new ClientPolicy(), provider, true);
			assertEquals(60000, merged.discoveryRefreshInterval);

			Files.copy(Paths.get(path("pass-through")), file, StandardCopyOption.REPLACE_EXISTING);
			assertTrue(file.toFile().setLastModified(file.toFile().lastModified() + 5000));
			assertTrue(provider.loadConfiguration());
			merged = new ClientPolicy(merged, provider, true);

			assertSame(PassThroughAddressTranslator.INSTANCE, merged.addressTranslator);
			assertEquals(SeedMergePolicy.DISCOVERY_ONLY, merged.discoveryMergePolicy);
			assertEquals(TranslationMissMode.PASS_THROUGH, merged.translationOnMiss);
			assertEquals(60000, merged.discoveryRefreshInterval);
		}
		finally {
			Files.deleteIfExists(file);
		}
	}

	@Test
	public void invalidReloadIsLoggedAndLastGoodConfigKept() throws Exception {
		Path file = Files.createTempFile("yaml-discovery", ".yaml");
		List<String> warnings = captureWarnings();

		try {
			Files.copy(Paths.get(path("reload-good")), file, StandardCopyOption.REPLACE_EXISTING);
			System.setProperty(CONFIG_SYS_PROP, file.toString());

			try (AerospikeClient client = new AerospikeClient(offline(), new Host("127.0.0.1", 1))) {
				assertEquals(TranslationMissMode.STRICT, client.getClientPolicy().translationOnMiss);
				assertEquals(99, client.getClientPolicy().maxConnsPerNode);
				assertEquals(2002, client.getClientPolicy().timeout);

				replace(file, "reload-invalid", 5000);
				String warning = await(() -> warnings.stream()
					.filter(m -> m.startsWith("Dynamic configuration failed") && m.contains("execution_mode")).findFirst().orElse(null));
				System.out.println("YAML-RELOAD-WARN " + warning);
				assertTrue(warning.contains("static.client.discovery.execution_mode is not supported: " +
					"the Java client runs discovery on the tend thread only"));
				assertEquals(TranslationMissMode.STRICT, client.getClientPolicy().translationOnMiss);
				assertEquals(99, client.getClientPolicy().maxConnsPerNode);
				assertEquals(2002, client.getClientPolicy().timeout);

				replace(file, "reload-changed", 10000);
				await(() -> (client.getClientPolicy().timeout == 4004)? Boolean.TRUE : null);
				assertEquals(TranslationMissMode.PASS_THROUGH, client.getClientPolicy().translationOnMiss);
				assertEquals(97, client.getClientPolicy().maxConnsPerNode);
			}
		}
		finally {
			Files.deleteIfExists(file);
		}
	}

	@Test
	public void invalidExecutionMode() throws Exception {
		assertInvalid("invalid-execution-mode",
			"static.client.discovery.execution_mode is not supported: the Java client runs discovery on the tend thread only");
	}

	@Test
	public void invalidRefreshTimeout() throws Exception {
		assertInvalid("invalid-refresh-timeout",
			"static.client.discovery.refresh_timeout_ms is not supported: the Java client runs discovery on the tend thread only");
	}

	@Test
	public void invalidDiscoveryProvider() throws Exception {
		assertInvalid("invalid-discovery-provider-value",
			"Invalid static.client.discovery.provider value 'consul': valid values: static");
	}

	@Test
	public void invalidDnsProvider() throws Exception {
		assertInvalid("invalid-provider-dns", "static.client.discovery.dns is required with provider dns");
	}

	@Test
	public void invalidAwsTagsProvider() throws Exception {
		assertInvalid("invalid-provider-aws-tags", "static.client.discovery.aws is required with provider aws-tags");
	}

	@Test
	public void invalidEnumValues() throws Exception {
		assertInvalid("invalid-merge-policy-value",
			"Invalid static.client.discovery.merge_policy value 'union': valid values: merge, replace, discovery_only");
		assertInvalid("invalid-translation-provider-value",
			"Invalid static.client.translation.provider value 'nat': valid values: static-map, port-map, pass-through");
		assertInvalid("invalid-on-miss-value",
			"Invalid static.client.translation.on_miss value 'drop': valid values: pass_through, strict");
	}

	@Test
	public void invalidMapForWrongProvider() throws Exception {
		assertInvalid("invalid-static-map-wrong-provider",
			"static.client.translation.static_map is only valid with provider static-map, not 'pass-through'");
		assertInvalid("invalid-port-map-wrong-provider",
			"static.client.translation.port_map is only valid with provider port-map, not 'static-map'");
		assertInvalid("invalid-port-map-missing", "static.client.translation.port_map is required with provider port-map");
	}

	@Test
	public void invalidPortMapEntry() throws Exception {
		assertInvalid("invalid-port-map-entry", "static.client.translation.port_map: Invalid port-map entry 10.0.0.1:3000");
	}

	@Test
	public void invalidStaticMapWithIpMap() throws Exception {
		assertInvalid("invalid-static-map-with-ip-map",
			"static.client.translation.static_map and static.client.ip_map are both set");
	}

	@Test
	public void invalidValueTypes() throws Exception {
		assertInvalid("invalid-refresh-interval-type",
			"Invalid static.client.discovery.refresh_interval_ms value 'thirty seconds': expected an integer");
		assertInvalid("invalid-deadline-type",
			"Invalid static.client.discovery.tend_refresh_deadline_ms value '1.5': expected an integer");
		assertInvalid("invalid-use-services-alternate-type",
			"Invalid static.client.use_services_alternate value 'maybe': expected true or false");
		assertInvalid("invalid-ip-map-type", "Invalid static.client.ip_map value '10.0.0.1': expected a map");
		assertInvalid("invalid-ip-map-value-type", "Invalid static.client.ip_map.10.0.0.1 value '3000': expected a string");
		assertInvalid("invalid-discovery-type", "Invalid static.client.discovery value 'static': expected a map");
	}

	private static void assertInvalid(String name, String message) throws Exception {
		String path = path(name);
		List<String> warnings = captureWarnings();

		assertNull(YamlConfigProvider.getConfigProvider(path));
		String warning = warnings.stream().filter(m -> m.contains(message)).findFirst().orElse(null);
		System.out.println("YAML-INVALID " + name + ": " + warnings);
		assertNotNull("no warning containing '" + message + "' in " + warnings, warning);
		assertTrue(warning, warning.contains("Failed to parse file://" + path));

		ClientPolicy code = offline();
		code.maxConnsPerNode = 77;
		code.discoveryMergePolicy = SeedMergePolicy.REPLACE;
		code.discoveryRefreshInterval = 45000;
		code.translationOnMiss = TranslationMissMode.STRICT;
		System.setProperty(CONFIG_SYS_PROP, path);

		try (AerospikeClient client = new AerospikeClient(code, new Host("127.0.0.1", 1))) {
			ClientPolicy policy = client.getClientPolicy();

			assertNull(client.getConfigProvider());
			assertEquals(77, policy.maxConnsPerNode);
			assertEquals(SeedMergePolicy.REPLACE, policy.discoveryMergePolicy);
			assertEquals(45000, policy.discoveryRefreshInterval);
			assertEquals(TranslationMissMode.STRICT, policy.translationOnMiss);
			assertNull(policy.ipMap);
			assertNull(policy.addressTranslator);
			assertFalse(policy.useServicesAlternate);
		}
		finally {
			System.clearProperty(CONFIG_SYS_PROP);
		}
	}

	private static ClientPolicy offline() {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		return policy;
	}

	private static List<String> captureWarnings() {
		List<String> warnings = new CopyOnWriteArrayList<>();
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
		});
		return warnings;
	}

	private static void replace(Path file, String name, long ageMillis) throws Exception {
		Files.copy(Paths.get(path(name)), file, StandardCopyOption.REPLACE_EXISTING);
		assertTrue(file.toFile().setLastModified(System.currentTimeMillis() + ageMillis));
	}

	private static <T> T await(Supplier<T> condition) throws Exception {
		long deadline = System.currentTimeMillis() + 10000;
		T value;

		while ((value = condition.get()) == null) {
			if (System.currentTimeMillis() > deadline) {
				fail("condition not met within 10s");
			}
			Thread.sleep(50);
		}
		return value;
	}

	private static void assertDiscoveryFields(ClientPolicy expected, ClientPolicy actual) {
		assertEquals(expected.ipMap, actual.ipMap);
		assertEquals(expected.useServicesAlternate, actual.useServicesAlternate);
		assertEquals(expected.seedCandidateProvider, actual.seedCandidateProvider);
		assertEquals(expected.addressTranslator, actual.addressTranslator);
		assertEquals(expected.translationOnMiss, actual.translationOnMiss);
		assertEquals(expected.discoveryMergePolicy, actual.discoveryMergePolicy);
		assertEquals(expected.discoveryRefreshInterval, actual.discoveryRefreshInterval);
		assertEquals(expected.discoveryTendRefreshDeadline, actual.discoveryTendRefreshDeadline);
		assertFalse(actual.useServicesAlternate);
	}
}
