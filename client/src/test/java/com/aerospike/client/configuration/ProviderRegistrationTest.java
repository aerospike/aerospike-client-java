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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.discovery.AddressTranslator;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.SeedCandidateContext;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;

public class ProviderRegistrationTest {
	private static final String CONFIG_SYS_PROP = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";

	private final List<Map<String,Object>> providerOptions = new CopyOnWriteArrayList<>();
	private final List<Map<String,Object>> translatorOptions = new CopyOnWriteArrayList<>();

	static final class RecordingProvider implements SeedCandidateProvider {
		final Map<String,Object> options;
		final List<SeedCandidateContext> contexts = new CopyOnWriteArrayList<>();

		RecordingProvider(Map<String,Object> options) {
			this.options = options;
		}

		@Override
		public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
			contexts.add(ctx);
			return List.of();
		}

		@Override
		public boolean needsPeriodicRefresh() {
			return false;
		}

		@Override
		public boolean supportsDeadline() {
			return true;
		}
	}

	static final class TableTranslator implements AddressTranslator {
		final Map<String,Object> options;

		TableTranslator(Map<String,Object> options) {
			this.options = options;
		}

		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.empty();
		}
	}

	@After
	public void reset() {
		System.clearProperty(CONFIG_SYS_PROP);
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private static String path(String name) throws Exception {
		return Paths.get(ProviderRegistrationTest.class.getResource("/yaml-config/" + name + ".yaml").toURI()).toString();
	}

	private ClientPolicy registered() {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.registerSeedCandidateProvider("consul", options -> {
			providerOptions.add(options);
			return new RecordingProvider(options);
		});
		policy.registerAddressTranslator("nat-table", options -> {
			translatorOptions.add(options);
			return new TableTranslator(options);
		});
		return policy;
	}

	@Test
	public void registeredProviderAndTranslatorBuiltFromYamlWithOptions() throws Exception {
		ClientPolicy policy = new ClientPolicy(registered(), new YamlConfigProvider(path("registered")));

		assertTrue(policy.seedCandidateProvider instanceof RecordingProvider);
		assertEquals(Map.of("merge_policy", "merge", "service", "aerospike", "endpoints", List.of("127.0.0.1:1")),
			((RecordingProvider)policy.seedCandidateProvider).options);
		assertEquals(SeedMergePolicy.MERGE, policy.discoveryMergePolicy);

		assertTrue(policy.addressTranslator instanceof TableTranslator);
		assertEquals(Map.of("on_miss", "strict", "table", Map.of("10.0.0.1:3000", "127.0.0.1:1")),
			((TableTranslator)policy.addressTranslator).options);
		assertEquals(TranslationMissMode.STRICT, policy.translationOnMiss);
		assertEquals(99, policy.maxConnsPerNode);
	}

	@Test
	public void nameOnlyGivesEmptyOptions() throws Exception {
		ClientPolicy policy = new ClientPolicy(registered(), new YamlConfigProvider(path("registered-name-only")));

		assertEquals(Map.of(), ((RecordingProvider)policy.seedCandidateProvider).options);
		assertEquals(Map.of(), ((TableTranslator)policy.addressTranslator).options);
	}

	@Test
	public void clientUsesRegisteredProviderAndTranslatorFromYaml() throws Exception {
		System.setProperty(CONFIG_SYS_PROP, path("registered"));

		try (AerospikeClient client = new AerospikeClient(registered(), new Host("127.0.0.1", 1))) {
			ClientPolicy policy = client.getClientPolicy();
			RecordingProvider provider = (RecordingProvider)policy.seedCandidateProvider;

			assertNotNull(client.getConfigProvider());
			assertEquals(1, provider.contexts.size());
			assertEquals(List.of(new Host("127.0.0.1", 1)), provider.contexts.get(0).configuredSeeds);
			assertTrue(policy.addressTranslator instanceof TableTranslator);
			assertEquals(1, providerOptions.size());
			assertEquals(1, translatorOptions.size());
		}
	}

	@Test
	public void restorePolicyDefaultsWithRegisteredNameUsesDefaults() throws Exception {
		List<String> warnings = new CopyOnWriteArrayList<>();
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, msg) -> {
			if (level == Log.Level.WARN) {
				warnings.add(msg);
			}
		});
		System.setProperty(CONFIG_SYS_PROP, path("registered"));

		try (AerospikeClient client = new AerospikeClient(registered(), new Host("127.0.0.1", 1))) {
			assertEquals(99, client.getClientPolicy().maxConnsPerNode);
			warnings.clear();

			client.restorePolicyDefaults();

			ClientPolicy policy = client.getClientPolicy();
			List<String> matching = new ArrayList<>();

			for (String warning : warnings) {
				if (warning.startsWith("Failed to parse " + path("registered"))) {
					matching.add(warning);
				}
			}
			assertEquals(warnings.toString(), 1, matching.size());
			assertTrue(matching.get(0), matching.get(0).contains("static.client.discovery.provider value 'consul'"));
			assertEquals(new ClientPolicy().maxConnsPerNode, policy.maxConnsPerNode);
			assertNull(policy.seedCandidateProvider);
			assertNull(policy.addressTranslator);
			assertEquals(1, providerOptions.size());
		}
	}

	@Test
	public void copyConstructorCopiesRegistrations() throws Exception {
		ClientPolicy copy = new ClientPolicy(registered());
		ClientPolicy policy = new ClientPolicy(copy, new YamlConfigProvider(path("registered")));

		assertTrue(policy.seedCandidateProvider instanceof RecordingProvider);
		assertTrue(policy.addressTranslator instanceof TableTranslator);

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> copy.registerSeedCandidateProvider("consul", RecordingProvider::new));
		assertTrue(ae.getMessage(), ae.getMessage().contains("'consul': already registered"));
	}

	@Test
	public void reloadReusesInstances() throws Exception {
		Path file = Files.createTempFile("yaml-registration", ".yaml");

		try {
			Files.copy(Paths.get(path("registered")), file, StandardCopyOption.REPLACE_EXISTING);
			YamlConfigProvider provider = new YamlConfigProvider(file.toString());
			ClientPolicy merged = new ClientPolicy(registered(), provider, true);
			SeedCandidateProvider seedProvider = merged.seedCandidateProvider;
			AddressTranslator translator = merged.addressTranslator;

			assertTrue(file.toFile().setLastModified(file.toFile().lastModified() + 5000));
			assertTrue(provider.loadConfiguration());
			merged = new ClientPolicy(merged, provider, true);

			assertSame(seedProvider, merged.seedCandidateProvider);
			assertSame(translator, merged.addressTranslator);
			assertEquals(1, providerOptions.size());
			assertEquals(1, translatorOptions.size());

			Files.copy(Paths.get(path("registered-name-only")), file, StandardCopyOption.REPLACE_EXISTING);
			assertTrue(file.toFile().setLastModified(file.toFile().lastModified() + 10000));
			assertTrue(provider.loadConfiguration());
			merged = new ClientPolicy(merged, provider, true);

			assertSame(seedProvider, merged.seedCandidateProvider);
			assertSame(translator, merged.addressTranslator);
			assertEquals(1, providerOptions.size());
			assertEquals(1, translatorOptions.size());
		}
		finally {
			Files.deleteIfExists(file);
		}
	}

	@Test
	public void builtInNameRejectedAtRegistration() {
		ClientPolicy policy = new ClientPolicy();

		for (String name : List.of("static", "dns", "aws-tags")) {
			AerospikeException ae = assertThrows(AerospikeException.class,
				() -> policy.registerSeedCandidateProvider(name, RecordingProvider::new));
			assertEquals(ResultCode.CLIENT_ERROR, ae.getResultCode());
			assertTrue(ae.getMessage(), ae.getMessage().contains("'" + name + "': built-in name"));
		}

		for (String name : List.of("static-map", "port-map", "pass-through")) {
			AerospikeException ae = assertThrows(AerospikeException.class,
				() -> policy.registerAddressTranslator(name, TableTranslator::new));
			assertEquals(ResultCode.CLIENT_ERROR, ae.getResultCode());
			assertTrue(ae.getMessage(), ae.getMessage().contains("'" + name + "': built-in name"));
		}
	}

	@Test
	public void duplicateNameRejectedAtRegistration() {
		ClientPolicy policy = new ClientPolicy();
		policy.registerSeedCandidateProvider("consul", RecordingProvider::new);
		policy.registerAddressTranslator("nat-table", TableTranslator::new);

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> policy.registerSeedCandidateProvider("consul", RecordingProvider::new));
		assertEquals(ResultCode.CLIENT_ERROR, ae.getResultCode());
		assertTrue(ae.getMessage(), ae.getMessage().contains("'consul': already registered"));

		ae = assertThrows(AerospikeException.class,
			() -> policy.registerAddressTranslator("nat-table", TableTranslator::new));
		assertEquals(ResultCode.CLIENT_ERROR, ae.getResultCode());
		assertTrue(ae.getMessage(), ae.getMessage().contains("'nat-table': already registered"));
	}

	@Test
	public void invalidRegistrationArgumentsRejected() {
		ClientPolicy policy = new ClientPolicy();

		assertThrows(AerospikeException.class, () -> policy.registerSeedCandidateProvider(null, RecordingProvider::new));
		assertThrows(AerospikeException.class, () -> policy.registerSeedCandidateProvider("", RecordingProvider::new));
		assertThrows(AerospikeException.class, () -> policy.registerSeedCandidateProvider("consul", null));
		assertThrows(AerospikeException.class, () -> policy.registerAddressTranslator(null, TableTranslator::new));
		assertThrows(AerospikeException.class, () -> policy.registerAddressTranslator("nat-table", null));
	}

	@Test
	public void unregisteredNameIsInvalidSetting() throws Exception {
		assertInvalid(new ClientPolicy(), "registered",
			"Invalid static.client.discovery.provider value 'consul': valid values: static");

		ClientPolicy other = new ClientPolicy();
		other.registerSeedCandidateProvider("consul", RecordingProvider::new);
		other.registerAddressTranslator("nat-table", TableTranslator::new);
		assertInvalid(other, "registered-translator-unregistered",
			"Invalid static.client.translation.provider value 'nat': valid values: static-map, port-map, pass-through, nat-table");
	}

	@Test
	public void factoryFailureIsInvalidSetting() throws Exception {
		ClientPolicy policy = new ClientPolicy();
		policy.registerSeedCandidateProvider("consul", options -> {
			throw new IllegalStateException("no consul agent");
		});
		policy.registerAddressTranslator("nat-table", options -> null);

		assertInvalid(policy, "registered",
			"Invalid static.client.discovery.provider value 'consul': factory failed: java.lang.IllegalStateException: no consul agent");

		policy = new ClientPolicy();
		policy.registerSeedCandidateProvider("consul", RecordingProvider::new);
		policy.registerAddressTranslator("nat-table", options -> null);

		assertInvalid(policy, "registered",
			"Invalid static.client.translation.provider value 'nat-table': factory returned null");
	}

	private static void assertInvalid(ClientPolicy code, String name, String message) throws Exception {
		List<String> warnings = new CopyOnWriteArrayList<>();
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, msg) -> {
			if (level == Log.Level.WARN) {
				warnings.add(msg);
			}
		});

		code.failIfNotConnected = false;
		code.timeout = 100;
		code.maxConnsPerNode = 77;
		System.setProperty(CONFIG_SYS_PROP, path(name));

		try (AerospikeClient client = new AerospikeClient(code, new Host("127.0.0.1", 1))) {
			ClientPolicy policy = client.getClientPolicy();
			List<String> matching = new ArrayList<>();

			for (String warning : warnings) {
				if (warning.contains(message)) {
					matching.add(warning);
				}
			}
			System.out.println("REGISTRATION-INVALID " + name + ": " + matching);
			assertEquals(warnings.toString(), 1, matching.size());
			assertTrue(matching.get(0), matching.get(0).startsWith("Failed to parse " + path(name)));
			assertNull(client.getConfigProvider());
			assertEquals(77, policy.maxConnsPerNode);
			assertNull(policy.seedCandidateProvider);
			assertNull(policy.addressTranslator);
			assertFalse(policy.translationOnMiss == TranslationMissMode.STRICT);
		}
		finally {
			System.clearProperty(CONFIG_SYS_PROP);
		}
	}
}
