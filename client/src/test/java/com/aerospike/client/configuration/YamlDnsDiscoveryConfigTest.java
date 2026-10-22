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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.DnsSeedCandidateProvider;
import com.aerospike.client.discovery.DnsSeedCandidateProvider.RecordType;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.policy.ClientPolicy;

public class YamlDnsDiscoveryConfigTest {
	private static final String CONFIG_SYS_PROP = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";

	@After
	public void reset() {
		System.clearProperty(CONFIG_SYS_PROP);
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private static String path(String name) throws Exception {
		return Paths.get(YamlDnsDiscoveryConfigTest.class.getResource("/yaml-config/" + name + ".yaml").toURI()).toString();
	}

	private static ClientPolicy offline() {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		return policy;
	}

	@Test
	public void dnsBlockBuildsProviderWithEveryKey() throws Exception {
		ClientPolicy policy = new ClientPolicy(new ClientPolicy(), YamlConfigProvider.getConfigProvider(path("dns-all")));
		DnsSeedCandidateProvider provider = (DnsSeedCandidateProvider)policy.seedCandidateProvider;
		System.out.println("YAML-DNS-ALL names=" + provider.getNames() + " recordType=" + provider.getRecordType() +
			" port=" + provider.getPort() + " tlsName=" + provider.getTlsName() + " timeout=" + provider.getTimeout() +
			" servers=" + provider.getServers());

		assertEquals(List.of("seeds-a.example.com", "seeds-b.example.com."), provider.getNames());
		assertEquals(RecordType.SRV, provider.getRecordType());
		assertEquals(3100, provider.getPort());
		assertEquals("tls-seed", provider.getTlsName());
		assertEquals(50, provider.getTimeout());
		assertEquals(List.of("127.0.0.1:5353", "[::1]:5353"), provider.getServers());
		assertEquals(SeedMergePolicy.REPLACE, policy.discoveryMergePolicy);
		assertEquals(99, policy.maxConnsPerNode);
	}

	@Test
	public void dnsBlockDefaults() throws Exception {
		ClientPolicy policy = new ClientPolicy(new ClientPolicy(), YamlConfigProvider.getConfigProvider(path("dns-defaults")));
		DnsSeedCandidateProvider provider = (DnsSeedCandidateProvider)policy.seedCandidateProvider;

		assertEquals(List.of("seeds.example.com"), provider.getNames());
		assertEquals(RecordType.A, provider.getRecordType());
		assertEquals(DnsSeedCandidateProvider.DEFAULT_PORT, provider.getPort());
		assertNull(provider.getTlsName());
		assertEquals(DnsSeedCandidateProvider.DEFAULT_TIMEOUT, provider.getTimeout());
	}

	@Test
	public void invalidDnsBlock() throws Exception {
		String block = "static.client.discovery.dns";

		assertInvalid("invalid-provider-dns", block + " is required with provider dns");
		assertInvalid("invalid-dns-without-provider", block + " is only valid with provider dns");
		assertInvalid("invalid-dns-static-provider", block + " is only valid with provider dns");
		assertInvalid("invalid-dns-block-type", "Invalid " + block + " value 'seeds.example.com': expected a map");
		assertInvalid("invalid-dns-names-missing", block + ".names is required");
		assertInvalid("invalid-dns-names-empty", "Invalid " + block + ".names value '[]': expected a non-empty list of strings");
		assertInvalid("invalid-dns-names-type",
			"Invalid " + block + ".names value 'seeds.example.com': expected a non-empty list of strings");
		assertInvalid("invalid-dns-record-type", "Invalid " + block + ".record_type value 'mx': valid values: a, srv");
		assertInvalid("invalid-dns-port-type", "Invalid " + block + ".port value '3000x': expected an integer");
		assertInvalid("invalid-dns-timeout-type", "Invalid " + block + ".timeout_ms value '1.5': expected an integer");
		assertInvalid("invalid-dns-port-range", "Invalid " + block + ".port value '70000': expected an integer from 1 to 65535");
		assertInvalid("invalid-dns-timeout-range",
			"Invalid " + block + ".timeout_ms value '0': expected an integer from 1 to " + Integer.MAX_VALUE);
		assertInvalid("invalid-dns-tls-name-type", "Invalid " + block + ".tls_name value '5': expected a string");
		assertInvalid("invalid-dns-servers-type",
			"Invalid " + block + ".servers value '127.0.0.1': expected a non-empty list of strings");
	}

	@Test
	public void valueRejectedByProviderMakesFileInvalidAtConstruction() throws Exception {
		List<String> warnings = captureWarnings();
		String path = path("invalid-dns-server-address");
		ClientPolicy code = offline();
		code.maxConnsPerNode = 77;
		System.setProperty(CONFIG_SYS_PROP, path);

		try (AerospikeClient client = new AerospikeClient(code, new Host("127.0.0.1", 1))) {
			String warning = warnings.stream().filter(m -> m.startsWith("Failed to parse " + path)).findFirst().orElse(null);
			System.out.println("YAML-DNS-INVALID invalid-dns-server-address: " + warning);

			assertNotNull(warnings.toString(), warning);
			assertTrue(warning, warning.contains("Invalid static.client.discovery.dns: Invalid DNS seed provider server: " +
				"'seeds.example.com': expected an IP address"));
			assertNull(client.getConfigProvider());
			assertNull(client.getClientPolicy().seedCandidateProvider);
			assertEquals(77, client.getClientPolicy().maxConnsPerNode);
		}
	}

	@Test
	public void reloadWithChangedDnsSettingsKeepsInstance() throws Exception {
		Path file = Files.createTempFile("yaml-dns", ".yaml");

		try {
			Files.copy(Paths.get(path("dns-reload-good")), file, StandardCopyOption.REPLACE_EXISTING);
			YamlConfigProvider provider = YamlConfigProvider.getConfigProvider(file.toString());
			ClientPolicy merged = new ClientPolicy(new ClientPolicy(), provider, true);
			SeedCandidateProvider dns = merged.seedCandidateProvider;
			assertEquals(List.of("seeds.example.com"), ((DnsSeedCandidateProvider)dns).getNames());

			Files.copy(Paths.get(path("dns-reload-changed")), file, StandardCopyOption.REPLACE_EXISTING);
			assertTrue(file.toFile().setLastModified(file.toFile().lastModified() + 5000));
			assertTrue(provider.loadConfiguration());
			merged = new ClientPolicy(merged, provider, true);

			assertEquals(97, merged.maxConnsPerNode);
			assertSame(dns, merged.seedCandidateProvider);
			assertEquals(List.of("seeds.example.com"), ((DnsSeedCandidateProvider)dns).getNames());
			assertEquals(3100, ((DnsSeedCandidateProvider)dns).getPort());
		}
		finally {
			Files.deleteIfExists(file);
		}
	}

	@Test
	public void clientReloadKeepsDnsProvider() throws Exception {
		Path file = Files.createTempFile("yaml-dns", ".yaml");
		List<String> warnings = captureWarnings();

		try {
			Files.copy(Paths.get(path("dns-reload-good")), file, StandardCopyOption.REPLACE_EXISTING);
			System.setProperty(CONFIG_SYS_PROP, file.toString());

			try (AerospikeClient client = new AerospikeClient(offline(), new Host("127.0.0.1", 1))) {
				SeedCandidateProvider dns = client.getClientPolicy().seedCandidateProvider;
				assertTrue(dns instanceof DnsSeedCandidateProvider);
				assertEquals(2002, client.getClientPolicy().timeout);

				Files.copy(Paths.get(path("dns-reload-changed")), file, StandardCopyOption.REPLACE_EXISTING);
				assertTrue(file.toFile().setLastModified(System.currentTimeMillis() + 5000));
				await(() -> (client.getClientPolicy().timeout == 4004)? Boolean.TRUE : null);
				System.out.println("YAML-DNS-RELOAD same instance=" + (dns == client.getClientPolicy().seedCandidateProvider) +
					" names=" + ((DnsSeedCandidateProvider)client.getClientPolicy().seedCandidateProvider).getNames());

				assertSame(dns, client.getClientPolicy().seedCandidateProvider);
				assertEquals(97, client.getClientPolicy().maxConnsPerNode);
				assertTrue(warnings.toString(), warnings.stream().noneMatch(m -> m.contains("static.client.discovery")));
			}
		}
		finally {
			Files.deleteIfExists(file);
		}
	}

	private static void assertInvalid(String name, String message) throws Exception {
		String path = path(name);
		List<String> warnings = captureWarnings();

		assertNull(YamlConfigProvider.getConfigProvider(path));
		String warning = warnings.stream().filter(m -> m.contains(message)).findFirst().orElse(null);
		System.out.println("YAML-DNS-INVALID " + name + ": " + warnings);
		assertNotNull("no warning containing '" + message + "' in " + warnings, warning);
		assertTrue(warning, warning.contains("Failed to parse file://" + path));

		ClientPolicy code = offline();
		code.maxConnsPerNode = 77;
		System.setProperty(CONFIG_SYS_PROP, path);

		try (AerospikeClient client = new AerospikeClient(code, new Host("127.0.0.1", 1))) {
			assertNull(client.getConfigProvider());
			assertEquals(77, client.getClientPolicy().maxConnsPerNode);
			assertNull(client.getClientPolicy().seedCandidateProvider);
		}
		finally {
			System.clearProperty(CONFIG_SYS_PROP);
		}
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
}
