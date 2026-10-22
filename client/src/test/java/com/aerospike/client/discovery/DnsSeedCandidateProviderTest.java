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
package com.aerospike.client.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.DnsSeedCandidateProvider.RecordType;

public class DnsSeedCandidateProviderTest {
	private static final SeedCandidateContext CTX = new SeedCandidateContext(null, new Host[0], 0);
	private static final Pattern IP_LITERAL = Pattern.compile("^(\\d{1,3}(\\.\\d{1,3}){3}|[0-9a-fA-F:]*:[0-9a-fA-F:.]*)$");
	private static final long MARGIN = 50;

	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private StubDnsServer dns;

	@Before
	public void setUp() throws Exception {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
		});
		dns = new StubDnsServer();
	}

	@After
	public void tearDown() throws Exception {
		dns.close();
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private DnsSeedCandidateProvider.Builder builder(String... names) {
		return DnsSeedCandidateProvider.builder().names(names).servers(dns.address());
	}

	private static void assertIpLiterals(List<Host> hosts) {
		for (Host host : hosts) {
			assertTrue("not an IP literal: " + host.name, IP_LITERAL.matcher(host.name).matches());
		}
	}

	@Test
	public void aAndAaaaRecords() throws Exception {
		dns.a("seeds.example.com", "10.0.0.9", "10.0.0.2").aaaa("seeds.example.com", "2001:db8::1");
		DnsSeedCandidateProvider provider = builder("seeds.example.com").port(3100).build();

		List<Host> hosts = provider.refreshSeedCandidates(CTX);
		System.out.println("DNS-A " + hosts);

		assertEquals(List.of(new Host("10.0.0.2", 3100), new Host("10.0.0.9", 3100), new Host("2001:db8::1", 3100)),
			hosts);
		assertEquals(hosts, provider.refreshSeedCandidates(CTX));
		assertIpLiterals(hosts);
		assertTrue(warnings.toString(), warnings.isEmpty());
	}

	@Test
	public void defaultPortIs3000() throws Exception {
		dns.a("seeds.example.com", "10.0.0.1");
		DnsSeedCandidateProvider provider = builder("seeds.example.com").build();

		assertEquals(List.of(new Host("10.0.0.1", 3000)), provider.refreshSeedCandidates(CTX));
		assertEquals(RecordType.A, provider.getRecordType());
		assertEquals(DnsSeedCandidateProvider.DEFAULT_PORT, provider.getPort());
		assertEquals(DnsSeedCandidateProvider.DEFAULT_TIMEOUT, provider.getTimeout());
		assertNull(provider.getTlsName());
	}

	@Test
	public void multipleNamesInOrderAndDeduplicated() throws Exception {
		dns.a("b.example.com", "10.0.0.2", "10.0.0.3");
		dns.a("a.example.com", "10.0.0.1", "10.0.0.2");
		DnsSeedCandidateProvider provider = builder("b.example.com", "a.example.com.").build();

		List<Host> hosts = provider.refreshSeedCandidates(CTX);
		System.out.println("DNS-MULTI " + hosts);

		assertEquals(List.of(new Host("10.0.0.2", 3000), new Host("10.0.0.3", 3000), new Host("10.0.0.1", 3000)), hosts);
	}

	@Test
	public void srvTargetsFromAdditionalSectionResponse() throws Exception {
		dns.srv("_aerospike._tcp.example.com", 20, 5, 3002, "node2.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
			.srv("_aerospike._tcp.example.com", 10, 9, 3003, "node3.example.com")
			.additionalA("_aerospike._tcp.example.com", "node1.example.com", "10.0.1.1")
			.additionalA("_aerospike._tcp.example.com", "node2.example.com", "10.0.1.2")
			.additionalA("_aerospike._tcp.example.com", "node3.example.com", "10.0.1.3")
			.a("node1.example.com", "10.0.1.1")
			.a("node2.example.com", "10.0.1.2")
			.a("node3.example.com", "10.0.1.3");
		DnsSeedCandidateProvider provider = builder("_aerospike._tcp.example.com").recordType(RecordType.SRV)
			.port(4000).build();

		List<Host> hosts = provider.refreshSeedCandidates(CTX);
		System.out.println("DNS-SRV-ADDITIONAL " + hosts + " queries " + dns.queries());

		assertEquals(List.of(new Host("10.0.1.3", 3003), new Host("10.0.1.1", 3001), new Host("10.0.1.2", 3002)), hosts);
		assertIpLiterals(hosts);
		assertEquals(hosts, provider.refreshSeedCandidates(CTX));
	}

	@Test
	public void srvTargetsFromFollowUpQueries() throws Exception {
		dns.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3002, "node2.example.com")
			.a("node1.example.com", "10.0.1.1")
			.aaaa("node2.example.com", "2001:db8::2");
		DnsSeedCandidateProvider provider = builder("_aerospike._tcp.example.com").recordType(RecordType.SRV).build();

		List<Host> hosts = provider.refreshSeedCandidates(CTX);
		System.out.println("DNS-SRV-FOLLOWUP " + hosts + " queries " + dns.queries());

		assertEquals(List.of(new Host("10.0.1.1", 3001), new Host("2001:db8::2", 3002)), hosts);
		assertIpLiterals(hosts);
		assertTrue(dns.queries().toString(), dns.queries().contains("node1.example.com./" + StubDnsServer.A));
		assertTrue(dns.queries().toString(), dns.queries().contains("node2.example.com./" + StubDnsServer.AAAA));
	}

	@Test
	public void srvTargetThatDoesNotResolveIsDropped() throws Exception {
		dns.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3002, "gone.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3003, ".")
			.a("node1.example.com", "10.0.1.1");
		DnsSeedCandidateProvider provider = builder("_aerospike._tcp.example.com").recordType(RecordType.SRV).build();

		List<Host> hosts = provider.refreshSeedCandidates(CTX);
		System.out.println("DNS-SRV-DROPPED " + hosts + " warnings " + warnings);

		assertEquals(List.of(new Host("10.0.1.1", 3001)), hosts);
		assertEquals(warnings.toString(), 1, warnings.size());
		assertEquals("DNS seed name _aerospike._tcp.example.com SRV target gone.example.com. dropped: NXDOMAIN",
			warnings.get(0));
	}

	@Test
	public void partialFailureReturnsResolvedNamesAndWarns() throws Exception {
		dns.a("good.example.com", "10.0.0.1")
			.rcode("bad.example.com", StubDnsServer.A, StubDnsServer.SERVFAIL)
			.rcode("bad.example.com", StubDnsServer.AAAA, StubDnsServer.SERVFAIL);
		DnsSeedCandidateProvider provider = builder("missing.example.com", "good.example.com", "bad.example.com").build();

		List<Host> hosts = provider.refreshSeedCandidates(CTX);
		System.out.println("DNS-PARTIAL " + hosts + " warnings " + warnings);

		assertEquals(List.of(new Host("10.0.0.1", 3000)), hosts);
		assertEquals(List.of("DNS seed name missing.example.com failed: NXDOMAIN",
			"DNS seed name bad.example.com failed: SERVFAIL"), warnings);
	}

	@Test
	public void everyNameFailingThrowsNamingEach() throws Exception {
		dns.rcode("bad.example.com", StubDnsServer.A, StubDnsServer.SERVFAIL)
			.rcode("bad.example.com", StubDnsServer.AAAA, StubDnsServer.SERVFAIL)
			.srv("other.example.com", 1, 1, 1, "x.example.com");
		DnsSeedCandidateProvider provider = builder("missing.example.com", "bad.example.com", "other.example.com").build();

		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(CTX));
		System.out.println("DNS-ALL-FAIL " + ae.getMessage());

		assertEquals("DNS seed discovery failed for every name: missing.example.com: NXDOMAIN; " +
			"bad.example.com: SERVFAIL; other.example.com: empty answer", ae.getBaseMessage());
	}

	@Test
	public void unansweredQueryReturnsWithinTimeout() throws Exception {
		try (StubDnsServer silent = new StubDnsServer(true)) {
			int timeout = 80;
			DnsSeedCandidateProvider provider = DnsSeedCandidateProvider.builder()
				.names("a.example.com", "b.example.com")
				.timeout(timeout)
				.servers(silent.address())
				.build();

			for (int i = 0; i < 3; i++) {
				long begin = System.nanoTime();
				AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(CTX));
				long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
				System.out.println("DNS-UNANSWERED elapsed=" + elapsed + "ms timeout=" + timeout + "ms margin=" + MARGIN +
					"ms queries=" + silent.queries().size() + " message=" + ae.getBaseMessage());

				assertTrue("elapsed " + elapsed + "ms", elapsed <= timeout + MARGIN);
				assertEquals("DNS seed discovery failed for every name: a.example.com: timeout; b.example.com: timeout",
					ae.getBaseMessage());
			}
		}
	}

	@Test
	public void unansweredSrvTargetsShareOneBudget() throws Exception {
		dns.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3002, "node2.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3003, "node3.example.com");

		for (String target : List.of("node1.example.com", "node2.example.com", "node3.example.com")) {
			dns.rcode(target, StubDnsServer.A, StubDnsServer.NO_ANSWER).rcode(target, StubDnsServer.AAAA, StubDnsServer.NO_ANSWER);
		}

		int timeout = 60;
		DnsSeedCandidateProvider provider = builder("_aerospike._tcp.example.com").recordType(RecordType.SRV)
			.timeout(timeout).build();

		long begin = System.nanoTime();
		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(CTX));
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
		System.out.println("DNS-UNANSWERED-SRV elapsed=" + elapsed + "ms timeout=" + timeout + "ms margin=" + MARGIN +
			"ms message=" + ae.getBaseMessage() + " warnings=" + warnings);

		assertTrue("elapsed " + elapsed + "ms", elapsed <= timeout + MARGIN);
		assertEquals("DNS seed discovery failed for every name: _aerospike._tcp.example.com: no SRV target resolved",
			ae.getBaseMessage());
		assertEquals(warnings.toString(), 3, warnings.size());

		for (String warning : warnings) {
			assertTrue(warning, warning.endsWith("dropped: timeout"));
		}
	}

	@Test
	public void truncatedAnswerWithUnansweredTcpReturnsWithinTimeout() throws Exception {
		try (StubDnsServer silent = new StubDnsServer(true).hangTcp();
			StubDnsServer truncated = new StubDnsServer().a("a.example.com", "10.0.0.1").truncate().hangTcp()) {
			int timeout = 200;
			DnsSeedCandidateProvider provider = DnsSeedCandidateProvider.builder()
				.names("a.example.com")
				.timeout(timeout)
				.servers(silent.address(), truncated.address())
				.build();

			long begin = System.nanoTime();
			AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(CTX));
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
			System.out.println("DNS-TRUNCATED elapsed=" + elapsed + "ms timeout=" + timeout + "ms margin=" + MARGIN +
				"ms udp=" + silent.queries() + truncated.queries() + " message=" + ae.getBaseMessage());

			assertTrue("elapsed " + elapsed + "ms", elapsed <= timeout + MARGIN);
			assertFalse(truncated.queries().toString(), truncated.queries().isEmpty());
		}
	}

	@Test
	public void tlsNameCarriedOnEveryHost() throws Exception {
		dns.a("seeds.example.com", "10.0.0.1", "10.0.0.2")
			.srv("_aerospike._tcp.example.com", 10, 1, 4333, "seeds.example.com");

		for (RecordType type : RecordType.values()) {
			String name = (type == RecordType.A)? "seeds.example.com" : "_aerospike._tcp.example.com";
			DnsSeedCandidateProvider provider = builder(name).recordType(type).tlsName("aerospike-tls").build();
			List<Host> hosts = provider.refreshSeedCandidates(CTX);

			assertTrue(!hosts.isEmpty());
			for (Host host : hosts) {
				assertEquals("aerospike-tls", host.tlsName);
			}
			assertEquals("aerospike-tls", provider.getTlsName());
		}
	}

	@Test
	public void needsPeriodicRefreshAndSupportsDeadline() {
		DnsSeedCandidateProvider provider = builder("seeds.example.com").build();

		assertTrue(provider.needsPeriodicRefresh());
		assertTrue(provider.supportsDeadline());
	}

	@Test
	public void startsNoThread() throws Exception {
		dns.a("seeds.example.com", "10.0.0.1");
		DnsSeedCandidateProvider provider = builder("seeds.example.com").build();
		provider.refreshSeedCandidates(CTX);

		ThreadMXBean bean = ManagementFactory.getThreadMXBean();
		long started = bean.getTotalStartedThreadCount();

		for (int i = 0; i < 5; i++) {
			provider.refreshSeedCandidates(CTX);
		}
		assertEquals(started, bean.getTotalStartedThreadCount());
	}

	@Test
	public void systemResolversReadAtConstruction() {
		DnsSeedCandidateProvider provider = DnsSeedCandidateProvider.builder().names("seeds.example.com").build();

		assertEquals(List.of(), provider.getServers());
	}

	@Test
	public void invalidConfigRejectedAtConstruction() {
		assertRejected(b -> b.names(), "Invalid DNS seed provider names: at least one name is required");
		assertRejected(b -> b.names((List<String>)null), "Invalid DNS seed provider names: at least one name is required");
		assertRejected(b -> b.names("a.example.com", ""), "Invalid DNS seed provider name: ''");
		assertRejected(b -> b.names("a.example.com", "A.example.com"), "duplicate");
		assertRejected(b -> b.names("a.example.com", "a.example.com."), "duplicate name 'a.example.com.'");
		assertRejected(b -> b.recordType(null), "Invalid DNS seed provider recordType: null");
		assertRejected(b -> b.port(0), "Invalid DNS seed provider port: 0");
		assertRejected(b -> b.port(65536), "Invalid DNS seed provider port: 65536");
		assertRejected(b -> b.timeout(0), "Invalid DNS seed provider timeout: 0");
		assertRejected(b -> b.tlsName(""), "Invalid DNS seed provider tlsName: empty");
		assertRejected(b -> b.servers("dns.example.com"), "Invalid DNS seed provider server: 'dns.example.com': expected an IP address");
		assertRejected(b -> b.servers("10.0.0.256"), "Invalid DNS seed provider server: '10.0.0.256'");
		assertRejected(b -> b.servers("10.0.0.1:x"), "Invalid DNS seed provider server: '10.0.0.1:x': invalid port");
		assertRejected(b -> b.servers("10.0.0.1:70000"), "invalid port");
		assertRejected(b -> b.servers("[2001:db8::1"), "Invalid DNS seed provider server: '[2001:db8::1'");
		assertRejected(b -> b.servers("2001:db8::zz"), "expected an IP address");
		assertRejected(b -> b.servers((String)null), "Invalid DNS seed provider server: null");

		DnsSeedCandidateProvider provider = DnsSeedCandidateProvider.builder().names("a.example.com")
			.servers("10.0.0.1", "10.0.0.2:5353", "2001:db8::1", "[2001:db8::2]:5353", "[::1]").build();
		assertEquals(List.of("10.0.0.1", "10.0.0.2:5353", "2001:db8::1", "[2001:db8::2]:5353", "[::1]"), provider.getServers());
	}

	private static void assertRejected(Consumer<DnsSeedCandidateProvider.Builder> config, String message) {
		DnsSeedCandidateProvider.Builder builder = DnsSeedCandidateProvider.builder().names("seeds.example.com")
			.servers("127.0.0.1");
		config.accept(builder);

		AerospikeException ae = assertThrows(AerospikeException.class, builder::build);
		assertTrue(ae.getBaseMessage(), ae.getBaseMessage().contains(message));
	}
}
