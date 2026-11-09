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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * DnsResolver against the JDK DNS provider as the oracle, on the same stub responses, and on
 * malformed responses.
 */
public class DnsResolverTest {
	private static final int TIMEOUT = 1000;
	private static final long MARGIN = 50;
	private static final long HANG = 10000;

	private StubDnsServer dns;

	@Before
	public void setUp() throws Exception {
		dns = new StubDnsServer();
	}

	@After
	public void tearDown() throws Exception {
		dns.close();
	}

	private interface Query {
		List<String> run() throws NamingException;
	}

	private static Object outcome(Query query) {
		try {
			return query.run();
		}
		catch (NamingException e) {
			return e.getClass();
		}
	}

	private static List<String> jndi(String url, String name, int type) throws NamingException {
		String id = typeName(type);
		Hashtable<String,String> env = new Hashtable<>();
		env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
		env.put(Context.PROVIDER_URL, url);
		env.put("com.sun.jndi.dns.timeout.initial", Integer.toString(TIMEOUT));
		env.put("com.sun.jndi.dns.timeout.retries", "1");
		DirContext dir = new InitialDirContext(env);

		try {
			Attribute attr = dir.getAttributes(name, new String[] {id}).get(id);
			List<String> values = new ArrayList<>();

			if (attr != null) {
				NamingEnumeration<?> e = attr.getAll();

				while (e.hasMore()) {
					values.add(e.next().toString());
				}
			}
			return values;
		}
		finally {
			dir.close();
		}
	}

	private static List<String> resolver(String url, String name, int type) throws NamingException {
		return new DnsResolver(url).query(name, type, TIMEOUT, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2 * TIMEOUT));
	}

	private static String typeName(int type) {
		switch (type) {
		case StubDnsServer.A:
			return "A";
		case StubDnsServer.AAAA:
			return "AAAA";
		default:
			return "SRV";
		}
	}

	private static Object assertSameAsJndi(String url, String name, int type) {
		Object expected = outcome(() -> jndi(url, name, type));
		Object actual = outcome(() -> resolver(url, name, type));
		System.out.println("DNS-DIFF " + name + '/' + typeName(type) + " jndi=" + expected + " resolver=" + actual);
		assertEquals(name + '/' + typeName(type), expected, actual);
		return actual;
	}

	@Test
	public void aRecords() throws Exception {
		dns.a("a.example.com", "10.0.0.9", "10.0.0.2", "10.0.0.9", "255.0.128.1");

		assertEquals(Arrays.asList("10.0.0.9", "10.0.0.2", "255.0.128.1"),
			assertSameAsJndi("dns://" + dns.address(), "a.example.com.", StubDnsServer.A));
	}

	@Test
	public void aaaaRecords() throws Exception {
		int[][] addresses = new int[][] {
			{0x2001, 0xdb8, 0, 0, 0, 0, 0, 1},
			{0, 0, 0, 0, 0, 0, 0, 0},
			{0, 0, 0, 0, 0, 0, 0, 1},
			{0, 0, 0, 0, 0, 0, 0, 2},
			{0, 0, 0, 0, 0, 0, 0x0a00, 1},
			{0, 0, 0, 0, 0, 0xffff, 0x0a00, 1},
			{0, 0, 0, 0, 0, 1, 0, 0},
			{1, 0, 0, 2, 0, 0, 0, 3},
			{1, 0, 0, 2, 0, 0, 3, 4},
			{1, 0, 2, 3, 4, 5, 6, 7},
			{1, 2, 3, 4, 5, 6, 7, 8},
			{0xfe80, 0, 0, 0, 0, 0, 0, 0},
			{0, 0, 1, 0, 0, 0, 0, 0},
			{0xabcd, 0xef01, 0, 0x2345, 0, 0, 0x6789, 0},
		};

		for (int i = 0; i < addresses.length; i++) {
			ByteBuffer bb = ByteBuffer.allocate(16);

			for (int group : addresses[i]) {
				bb.putShort((short)group);
			}

			String name = "v6-" + i + ".example.com.";
			dns.answerRecord(name, StubDnsServer.AAAA, name, StubDnsServer.AAAA, bb.array());
			assertEquals(1, ((List<?>)assertSameAsJndi("dns://" + dns.address(), name, StubDnsServer.AAAA)).size());
		}
	}

	@Test
	public void srvRecords() throws Exception {
		dns.srv("_aerospike._tcp.example.com", 10, 1, 3001, "Node1.Example.com")
			.srv("_aerospike._tcp.example.com", 65535, 65535, 65535, "node2.example.com")
			.srv("_aerospike._tcp.example.com", 0, 0, 1, ".");

		assertEquals(Arrays.asList("10 1 3001 Node1.Example.com.", "65535 65535 65535 node2.example.com.", "0 0 1 ."),
			assertSameAsJndi("dns://" + dns.address(), "_aerospike._tcp.example.com.", StubDnsServer.SRV));
	}

	@Test
	public void cnameInAnswer() throws Exception {
		dns.cname("alias.example.com", StubDnsServer.A, "real.example.com")
			.answerRecord("alias.example.com", StubDnsServer.A, "real.example.com", StubDnsServer.A, new byte[] {10, 0, 0, 5})
			.cname("alias.example.com", StubDnsServer.AAAA, "real.example.com");

		assertEquals(Arrays.asList("10.0.0.5"),
			assertSameAsJndi("dns://" + dns.address(), "alias.example.com.", StubDnsServer.A));
		assertEquals(Arrays.asList(),
			assertSameAsJndi("dns://" + dns.address(), "alias.example.com.", StubDnsServer.AAAA));
	}

	@Test
	public void answersForOtherNames() throws Exception {
		dns.a("a.example.com", "10.0.0.1")
			.answerRecord("a.example.com", StubDnsServer.A, "other.example.com", StubDnsServer.A, new byte[] {10, 0, 0, 7})
			.answerRecord("a.example.com", StubDnsServer.A, "a.example.com", StubDnsServer.AAAA, new byte[16]);

		assertEquals(Arrays.asList("10.0.0.1", "10.0.0.7"),
			assertSameAsJndi("dns://" + dns.address(), "a.example.com.", StubDnsServer.A));
	}

	@Test
	public void emptyAnswer() throws Exception {
		dns.a("a.example.com", "10.0.0.1");

		assertEquals(Arrays.asList(), assertSameAsJndi("dns://" + dns.address(), "a.example.com.", StubDnsServer.AAAA));
	}

	@Test
	public void responseCodes() throws Exception {
		dns.rcode("servfail.example.com", StubDnsServer.A, StubDnsServer.SERVFAIL)
			.rcode("formerr.example.com", StubDnsServer.A, 1)
			.rcode("refused.example.com", StubDnsServer.A, 5)
			.rcode("rcode9.example.com", StubDnsServer.A, 9);

		assertEquals(javax.naming.NameNotFoundException.class,
			assertSameAsJndi("dns://" + dns.address(), "missing.example.com.", StubDnsServer.A));
		assertEquals(javax.naming.ServiceUnavailableException.class,
			assertSameAsJndi("dns://" + dns.address(), "servfail.example.com.", StubDnsServer.A));
		assertEquals(NamingException.class,
			assertSameAsJndi("dns://" + dns.address(), "formerr.example.com.", StubDnsServer.A));
		assertEquals(javax.naming.OperationNotSupportedException.class,
			assertSameAsJndi("dns://" + dns.address(), "refused.example.com.", StubDnsServer.A));
		assertEquals(NamingException.class,
			assertSameAsJndi("dns://" + dns.address(), "rcode9.example.com.", StubDnsServer.A));
	}

	@Test
	public void serversTriedInOrder() throws Exception {
		try (StubDnsServer second = new StubDnsServer()) {
			second.a("a.example.com", "10.0.0.2").a("servfail.example.com", "10.0.0.2").a("missing.example.com", "10.0.0.2");
			dns.a("a.example.com", "10.0.0.1")
				.rcode("servfail.example.com", StubDnsServer.A, StubDnsServer.SERVFAIL);
			String url = "dns://" + dns.address() + " dns://" + second.address();

			assertEquals(Arrays.asList("10.0.0.1"), assertSameAsJndi(url, "a.example.com.", StubDnsServer.A));
			assertEquals(Arrays.asList("10.0.0.2"), assertSameAsJndi(url, "servfail.example.com.", StubDnsServer.A));
			assertEquals(javax.naming.NameNotFoundException.class, assertSameAsJndi(url, "missing.example.com.", StubDnsServer.A));

			second.rcode("servfail.example.com", StubDnsServer.A, StubDnsServer.SERVFAIL);
			assertEquals(javax.naming.ServiceUnavailableException.class,
				assertSameAsJndi(url, "servfail.example.com.", StubDnsServer.A));
		}
	}

	@Test
	public void compressionPointers() throws Exception {
		// Question name at 12; "example" label of _aerospike._tcp.example.com at 12 + 11 + 5 = 28.
		dns.raw("_aerospike._tcp.example.com", StubDnsServer.SRV, query -> response(query, 2, out -> {
			out.write(new byte[] {(byte)0xC0, 12});
			rr(out, StubDnsServer.SRV, srv(10, 1, 3001, concat(label("node1"), new byte[] {(byte)0xC0, 28})));
			int node1 = query.length + 2 + 10 + 6;
			out.write(new byte[] {(byte)0xC0, 12});
			rr(out, StubDnsServer.SRV, srv(20, 2, 3002, concat(label("a.b"), new byte[] {(byte)(0xC0 | (node1 >> 8)), (byte)node1})));
		}));
		dns.raw("a.example.com", StubDnsServer.A, query -> response(query, 2, out -> {
			out.write(new byte[] {(byte)0xC0, 12});
			rr(out, StubDnsServer.A, new byte[] {10, 0, 0, 1});
			out.write(concat(label("www"), new byte[] {(byte)0xC0, 12}));
			rr(out, StubDnsServer.A, new byte[] {10, 0, 0, 2});
		}));

		assertEquals(Arrays.asList("10 1 3001 node1.example.com.", "20 2 3002 a\\.b.node1.example.com."),
			assertSameAsJndi("dns://" + dns.address(), "_aerospike._tcp.example.com.", StubDnsServer.SRV));
		assertEquals(Arrays.asList("10.0.0.1", "10.0.0.2"),
			assertSameAsJndi("dns://" + dns.address(), "a.example.com.", StubDnsServer.A));
	}

	@Test(timeout = HANG)
	public void malformedResponsesFail() throws Exception {
		dns.raw("short.example.com", StubDnsServer.A, query -> Arrays.copyOf(query, 5));
		dns.raw("loop.example.com", StubDnsServer.A, query -> response(query, 1, out -> {
			int self = query.length;
			out.write(new byte[] {(byte)(0xC0 | (self >> 8)), (byte)self});
			rr(out, StubDnsServer.A, new byte[] {10, 0, 0, 1});
		}));
		dns.raw("loop2.example.com", StubDnsServer.SRV, query -> response(query, 1, out -> {
			out.write(new byte[] {(byte)0xC0, 12});
			int target = query.length + 2 + 10 + 6;
			rr(out, StubDnsServer.SRV, srv(1, 1, 1, new byte[] {(byte)(0xC0 | (target >> 8)), (byte)target}));
		}));
		dns.raw("rdlength.example.com", StubDnsServer.A, query -> {
			byte[] response = response(query, 1, out -> {
				out.write(new byte[] {(byte)0xC0, 12});
				rr(out, StubDnsServer.A, new byte[] {10, 0, 0, 1});
			});
			response[response.length - 5] = 100;
			return response;
		});
		dns.raw("label.example.com", StubDnsServer.A, query -> response(query, 1, out -> out.write(new byte[] {20, 'a', 'b'})));
		dns.raw("srvlength.example.com", StubDnsServer.SRV, query -> response(query, 1, out -> {
			out.write(new byte[] {(byte)0xC0, 12});
			rr(out, StubDnsServer.SRV, new byte[] {0, 1, 0});
		}));
		dns.raw("query.example.com", StubDnsServer.A, query -> {
			byte[] response = response(query, 0, out -> {});
			response[2] &= 0x7F;
			return response;
		});
		dns.raw("wrongid.example.com", StubDnsServer.A, query -> {
			byte[] response = response(query, 1, out -> {
				out.write(new byte[] {(byte)0xC0, 12});
				rr(out, StubDnsServer.A, new byte[] {10, 0, 0, 1});
			});
			response[1] ^= 1;
			return response;
		});
		dns.raw("question.example.com", StubDnsServer.A, query -> {
			byte[] response = response(query, 1, out -> {
				out.write(new byte[] {(byte)0xC0, 12});
				rr(out, StubDnsServer.A, new byte[] {10, 0, 0, 1});
			});
			response[14] = 'x';
			return response;
		});

		String[][] cases = new String[][] {
			{"short.example.com.", "A"}, {"loop.example.com.", "A"}, {"loop2.example.com.", "SRV"},
			{"rdlength.example.com.", "A"}, {"label.example.com.", "A"}, {"srvlength.example.com.", "SRV"},
			{"query.example.com.", "A"}, {"wrongid.example.com.", "A"}, {"question.example.com.", "A"},
		};
		int timeout = 100;

		for (String[] c : cases) {
			int type = c[1].equals("A")? StubDnsServer.A : StubDnsServer.SRV;
			DnsResolver resolver = new DnsResolver("dns://" + dns.address());
			long begin = System.nanoTime();
			NamingException e = assertThrows(NamingException.class,
				() -> resolver.query(c[0], type, timeout, begin + TimeUnit.MILLISECONDS.toNanos(timeout)));
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
			System.out.println("DNS-MALFORMED " + c[0] + " elapsed=" + elapsed + "ms " + e + " root=" + e.getRootCause());

			assertTrue(c[0] + " elapsed " + elapsed + "ms", elapsed <= timeout + MARGIN);
		}
	}

	@Test
	public void serverUrls() throws Exception {
		assertEquals(4, new DnsResolver("dns://10.0.0.1:53 dns://[2001:db8::1]:5353 dns://192.168.1.1/example.com dns://[::1]")
			.serverCount());
		assertThrows(NamingException.class, () -> new DnsResolver("dns://10.0.0.1:x"));
	}

	@Test
	public void invalidNamesFail() throws Exception {
		char[] longLabel = new char[64];
		Arrays.fill(longLabel, 'a');
		DnsResolver resolver = new DnsResolver("dns://" + dns.address());
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);

		assertThrows(NamingException.class, () -> resolver.query("a..example.com.", StubDnsServer.A, 100, deadline));
		assertThrows(NamingException.class, () -> resolver.query(new String(longLabel) + ".com.", StubDnsServer.A, 100, deadline));
		assertThrows(NamingException.class, () -> resolver.query("Ā.com.", StubDnsServer.A, 100, deadline));
		assertTrue(dns.queries().isEmpty());
	}

	private interface Section {
		void write(DataOutputStream out) throws IOException;
	}

	private static byte[] response(byte[] query, int answers, Section section) {
		try {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			out.write(query, 0, 2);
			out.writeShort(0x8180);
			out.writeShort(1);
			out.writeShort(answers);
			out.writeShort(0);
			out.writeShort(0);
			out.write(query, 12, query.length - 12);
			section.write(out);
			return bytes.toByteArray();
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private static void rr(DataOutputStream out, int type, byte[] rdata) throws IOException {
		out.writeShort(type);
		out.writeShort(1);
		out.writeInt(60);
		out.writeShort(rdata.length);
		out.write(rdata);
	}

	private static byte[] srv(int priority, int weight, int port, byte[] target) {
		ByteBuffer bb = ByteBuffer.allocate(6 + target.length);
		bb.putShort((short)priority).putShort((short)weight).putShort((short)port).put(target);
		return bb.array();
	}

	private static byte[] label(String label) {
		byte[] bytes = label.getBytes(StandardCharsets.US_ASCII);
		return concat(new byte[] {(byte)bytes.length}, bytes);
	}

	private static byte[] concat(byte[] a, byte[] b) {
		byte[] c = Arrays.copyOf(a, a.length + b.length);
		System.arraycopy(b, 0, c, a.length, b.length);
		return c;
	}
}
