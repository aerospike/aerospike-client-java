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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider.AddressField;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.policy.ClientPolicy;

public class YamlAwsTagsDiscoveryConfigTest {
	private static final String CONFIG_SYS_PROP = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";

	@After
	public void reset() {
		System.clearProperty(CONFIG_SYS_PROP);
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private static String path(String name) throws Exception {
		return Paths.get(YamlAwsTagsDiscoveryConfigTest.class.getResource("/yaml-config/" + name + ".yaml").toURI()).toString();
	}

	private static ClientPolicy offline() {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		return policy;
	}

	@Test
	public void awsBlockBuildsProviderWithEveryKey() throws Exception {
		ClientPolicy policy = new ClientPolicy(new ClientPolicy(), YamlConfigProvider.getConfigProvider(path("aws-all")));
		AwsEc2TagSeedCandidateProvider provider = (AwsEc2TagSeedCandidateProvider)policy.seedCandidateProvider;
		System.out.println("YAML-AWS-ALL region=" + provider.getRegion() + " filters=" + provider.getFilters() +
			" addressField=" + provider.getAddressField() + " port=" + provider.getPort() + " tlsName=" +
			provider.getTlsName() + " timeout=" + provider.getTimeout() + " initTimeout=" + provider.getInitTimeout() +
			" endpoint=" + provider.getEndpoint());

		Map<String,List<String>> filters = new LinkedHashMap<>();
		filters.put("tag:aerospike-cluster", List.of("prod-a", "prod-b"));
		filters.put("subnet-id", List.of("subnet-0123"));
		assertEquals("eu-west-1", provider.getRegion());
		assertEquals(filters, provider.getFilters());
		assertEquals(AddressField.PUBLIC_DNS, provider.getAddressField());
		assertEquals(3100, provider.getPort());
		assertEquals("tls-seed", provider.getTlsName());
		assertEquals(60, provider.getTimeout());
		assertEquals(5000, provider.getInitTimeout());
		assertEquals("https://vpce-0123.ec2.eu-west-1.vpce.amazonaws.com", provider.getEndpoint());
		assertEquals(SeedMergePolicy.MERGE, policy.discoveryMergePolicy);
		assertEquals(99, policy.maxConnsPerNode);
	}

	@Test
	public void awsBlockDefaults() throws Exception {
		ClientPolicy policy = new ClientPolicy(new ClientPolicy(), YamlConfigProvider.getConfigProvider(path("aws-defaults")));
		AwsEc2TagSeedCandidateProvider provider = (AwsEc2TagSeedCandidateProvider)policy.seedCandidateProvider;

		assertEquals("us-east-1", provider.getRegion());
		assertEquals(Map.of("tag:aerospike-cluster", List.of("prod-a")), provider.getFilters());
		assertEquals(AddressField.PRIVATE_IP, provider.getAddressField());
		assertEquals(AwsEc2TagSeedCandidateProvider.DEFAULT_PORT, provider.getPort());
		assertNull(provider.getTlsName());
		assertEquals(AwsEc2TagSeedCandidateProvider.DEFAULT_TIMEOUT, provider.getTimeout());
		assertEquals(AwsEc2TagSeedCandidateProvider.DEFAULT_INIT_TIMEOUT, provider.getInitTimeout());
		assertNull(provider.getEndpoint());
	}

	@Test
	public void invalidAwsBlock() throws Exception {
		String block = "static.client.discovery.aws";

		assertInvalid("invalid-provider-aws-tags", block + " is required with provider aws-tags");
		assertInvalid("invalid-aws-without-provider", block + " is only valid with provider aws-tags");
		assertInvalid("invalid-aws-static-provider", block + " is only valid with provider aws-tags");
		assertInvalid("invalid-aws-dns-provider", block + " is only valid with provider aws-tags");
		assertInvalid("invalid-aws-block-type", "Invalid " + block + " value 'us-east-1': expected a map");
		assertInvalid("invalid-aws-region-missing", block + ".region is required");
		assertInvalid("invalid-aws-region-type", "Invalid " + block + ".region value '5': expected a string");
		assertInvalid("invalid-aws-filters-missing", block + ".filters is required");
		assertInvalid("invalid-aws-filters-empty",
			"Invalid " + block + ".filters value '[]': expected a non-empty list of name and values");
		assertInvalid("invalid-aws-filters-type",
			"Invalid " + block + ".filters value 'tag:a=b': expected a non-empty list of name and values");
		assertInvalid("invalid-aws-filter-type", "Invalid " + block + ".filters[0] value 'tag:a': expected a map");
		assertInvalid("invalid-aws-filter-name-missing", block + ".filters[0].name is required");
		assertInvalid("invalid-aws-filter-values-missing", block + ".filters[0].values is required");
		assertInvalid("invalid-aws-filter-values-empty",
			"Invalid " + block + ".filters[0].values value '[]': expected a non-empty list of strings");
		assertInvalid("invalid-aws-address-field",
			"Invalid " + block + ".address_field value 'ipv6': valid values: private_ip, public_ip, private_dns, public_dns");
		assertInvalid("invalid-aws-port-type", "Invalid " + block + ".port value '3000x': expected an integer");
		assertInvalid("invalid-aws-port-range", "Invalid " + block + ".port value '70000': expected an integer from 1 to 65535");
		assertInvalid("invalid-aws-timeout-type", "Invalid " + block + ".timeout_ms value '1.5': expected an integer");
		assertInvalid("invalid-aws-timeout-range",
			"Invalid " + block + ".timeout_ms value '0': expected an integer from 1 to " + Integer.MAX_VALUE);
		assertInvalid("invalid-aws-init-timeout-type", "Invalid " + block + ".init_timeout_ms value 'ten': expected an integer");
		assertInvalid("invalid-aws-tls-name-type", "Invalid " + block + ".tls_name value '5': expected a string");
		assertInvalid("invalid-aws-endpoint-type", "Invalid " + block + ".endpoint value '443': expected a string");
	}

	@Test
	public void valueRejectedByProviderMakesFileInvalidAtConstruction() throws Exception {
		List<String> warnings = captureWarnings();
		String path = path("invalid-aws-endpoint-scheme");
		ClientPolicy code = offline();
		code.maxConnsPerNode = 77;
		System.setProperty(CONFIG_SYS_PROP, path);

		try (AerospikeClient client = new AerospikeClient(code, new Host("127.0.0.1", 1))) {
			String warning = warnings.stream().filter(m -> m.startsWith("Failed to parse " + path)).findFirst().orElse(null);
			System.out.println("YAML-AWS-INVALID invalid-aws-endpoint-scheme: " + warning);

			assertNotNull(warnings.toString(), warning);
			assertTrue(warning, warning.contains("Invalid static.client.discovery.aws: Invalid AWS EC2 seed provider " +
				"endpoint: 'ftp://ec2.example.com': expected http or https URL with a host and no query"));
			assertNull(client.getConfigProvider());
			assertNull(client.getClientPolicy().seedCandidateProvider);
			assertEquals(77, client.getClientPolicy().maxConnsPerNode);
		}
	}

	@Test
	public void reloadWithChangedAwsSettingsKeepsInstance() throws Exception {
		Path file = Files.createTempFile("yaml-aws", ".yaml");

		try {
			Files.copy(Paths.get(path("aws-reload-good")), file, StandardCopyOption.REPLACE_EXISTING);
			YamlConfigProvider provider = YamlConfigProvider.getConfigProvider(file.toString());
			ClientPolicy merged = new ClientPolicy(new ClientPolicy(), provider, true);
			SeedCandidateProvider aws = merged.seedCandidateProvider;
			assertEquals("us-east-1", ((AwsEc2TagSeedCandidateProvider)aws).getRegion());

			Files.copy(Paths.get(path("aws-reload-changed")), file, StandardCopyOption.REPLACE_EXISTING);
			assertTrue(file.toFile().setLastModified(file.toFile().lastModified() + 5000));
			assertTrue(provider.loadConfiguration());
			merged = new ClientPolicy(merged, provider, true);
			System.out.println("YAML-AWS-RELOAD policy same instance=" + (aws == merged.seedCandidateProvider) +
				" region=" + ((AwsEc2TagSeedCandidateProvider)aws).getRegion() + " port=" +
				((AwsEc2TagSeedCandidateProvider)aws).getPort());

			assertEquals(97, merged.maxConnsPerNode);
			assertSame(aws, merged.seedCandidateProvider);
			assertEquals("us-east-1", ((AwsEc2TagSeedCandidateProvider)aws).getRegion());
			assertEquals(3100, ((AwsEc2TagSeedCandidateProvider)aws).getPort());
			assertEquals(AddressField.PRIVATE_IP, ((AwsEc2TagSeedCandidateProvider)aws).getAddressField());
		}
		finally {
			Files.deleteIfExists(file);
		}
	}

	@Test
	public void clientReloadKeepsAwsProvider() throws Exception {
		Path file = Files.createTempFile("yaml-aws", ".yaml");
		List<String> warnings = captureWarnings();

		try {
			Files.copy(Paths.get(path("aws-reload-good")), file, StandardCopyOption.REPLACE_EXISTING);
			System.setProperty(CONFIG_SYS_PROP, file.toString());
			ClientPolicy code = offline();
			code.seedCandidateProvider = null;

			try (AerospikeClient client = new AerospikeClient(code, new Host("127.0.0.1", 1))) {
				SeedCandidateProvider aws = client.getClientPolicy().seedCandidateProvider;
				assertTrue(aws instanceof AwsEc2TagSeedCandidateProvider);
				assertEquals(2002, client.getClientPolicy().timeout);

				Files.copy(Paths.get(path("aws-reload-changed")), file, StandardCopyOption.REPLACE_EXISTING);
				assertTrue(file.toFile().setLastModified(System.currentTimeMillis() + 5000));
				await(() -> (client.getClientPolicy().timeout == 4004)? Boolean.TRUE : null);
				System.out.println("YAML-AWS-RELOAD client same instance=" + (aws == client.getClientPolicy().seedCandidateProvider) +
					" region=" + ((AwsEc2TagSeedCandidateProvider)client.getClientPolicy().seedCandidateProvider).getRegion());

				assertSame(aws, client.getClientPolicy().seedCandidateProvider);
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
		System.out.println("YAML-AWS-INVALID " + name + ": " + warnings);
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
