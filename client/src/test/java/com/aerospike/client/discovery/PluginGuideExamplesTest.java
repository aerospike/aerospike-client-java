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
package com.aerospike.client.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;

public class PluginGuideExamplesTest {
	private static final String CONFIG_SYS_PROP = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";

	private static final String YAML = """
		version: 1.2.0
		static:
		  client:
		    discovery:
		      provider: host-list
		      merge_policy: merge
		      hosts:
		        - "192.0.2.1:3000"
		        - "192.0.2.2:3000"
		    translation:
		      provider: nat-table
		      on_miss: strict
		      table:
		        "192.0.2.1:3000": "lb.example.com:30001"
		        "192.0.2.2:3000": "lb.example.com:30002"
		""";

	static final class HostListSeedProvider implements SeedCandidateProvider {
		private volatile List<Host> hosts;

		HostListSeedProvider(Map<String,Object> options) {
			List<Host> list = new ArrayList<>();

			for (Object host : (List<?>)options.get("hosts")) {
				list.addAll(Host.parseServiceHosts((String)host));
			}
			this.hosts = List.copyOf(list);
		}

		void setHosts(List<Host> hosts) {
			this.hosts = List.copyOf(hosts);
		}

		@Override
		public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
			return hosts;
		}

		@Override
		public boolean needsPeriodicRefresh() {
			return true;
		}

		@Override
		public boolean supportsDeadline() {
			return true;
		}
	}

	static final class NatTableTranslator implements AddressTranslator {
		private final Map<String,Host> table = new HashMap<>();

		NatTableTranslator(Map<String,Object> options) {
			Map<?,?> map = (Map<?,?>)options.get("table");

			for (Map.Entry<?,?> entry : map.entrySet()) {
				table.put((String)entry.getKey(), Host.parseServiceHosts((String)entry.getValue()).get(0));
			}
		}

		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.ofNullable(table.get(advertised.host + ':' + advertised.port));
		}
	}

	@After
	public void reset() {
		System.clearProperty(CONFIG_SYS_PROP);
	}

	@Test
	public void registeredProviderAndTranslatorSelectedFromYaml() throws Exception {
		Path file = Files.createTempFile("plugin-guide", ".yaml");

		try {
			Files.writeString(file, YAML);
			System.setProperty(CONFIG_SYS_PROP, file.toString());

			ClientPolicy policy = new ClientPolicy();
			policy.registerSeedCandidateProvider("host-list", HostListSeedProvider::new);
			policy.registerAddressTranslator("nat-table", NatTableTranslator::new);

			policy.failIfNotConnected = false;
			policy.timeout = 100;

			try (AerospikeClient client = new AerospikeClient(policy, new Host("192.0.2.10", 3000))) {
				ClientPolicy merged = client.getClientPolicy();

				assertTrue(merged.seedCandidateProvider instanceof HostListSeedProvider);
				assertTrue(merged.addressTranslator instanceof NatTableTranslator);
				assertEquals(SeedMergePolicy.MERGE, merged.discoveryMergePolicy);
				assertEquals(TranslationMissMode.STRICT, merged.translationOnMiss);

				SeedCandidateContext ctx = new SeedCandidateContext(null, new Host[0], 0);
				assertEquals(List.of(new Host("192.0.2.1", 3000), new Host("192.0.2.2", 3000)),
					merged.seedCandidateProvider.refreshSeedCandidates(ctx));

				Optional<Host> hit = merged.addressTranslator.translate(
					new Endpoint("BB9020011AC4202", null, "192.0.2.2", 3000, Endpoint.SourceList.STANDARD));
				assertEquals(new Host("lb.example.com", 30002), hit.get());
			}
		}
		finally {
			Files.deleteIfExists(file);
		}
	}

	@Test
	public void providerReturnsHostsSetByApplication() {
		HostListSeedProvider provider = new HostListSeedProvider(Map.of("hosts", List.of("192.0.2.1:3000")));
		SeedCandidateContext ctx = new SeedCandidateContext(null, new Host[0], 0);

		assertTrue(provider.needsPeriodicRefresh());
		assertTrue(provider.supportsDeadline());
		assertEquals(List.of(new Host("192.0.2.1", 3000)), provider.refreshSeedCandidates(ctx));

		provider.setHosts(List.of(new Host("192.0.2.3", 3000), new Host("192.0.2.4", 3000)));
		assertEquals(List.of(new Host("192.0.2.3", 3000), new Host("192.0.2.4", 3000)),
			provider.refreshSeedCandidates(new SeedCandidateContext(null, new Host[0], 30)));
	}

	@Test
	public void translatorHitAndMiss() {
		NatTableTranslator translator = new NatTableTranslator(
			Map.of("table", Map.of("192.0.2.1:3000", "lb.example.com:30001")));

		Optional<Host> hit = translator.translate(
			new Endpoint("BB9010011AC4202", "node1.example.com", "192.0.2.1", 3000, Endpoint.SourceList.ALTERNATE));
		assertEquals("lb.example.com", hit.get().name);
		assertEquals(30001, hit.get().port);

		Optional<Host> miss = translator.translate(
			new Endpoint("BB9030011AC4202", null, "192.0.2.3", 3000, Endpoint.SourceList.STANDARD));
		assertFalse(miss.isPresent());
	}
}
