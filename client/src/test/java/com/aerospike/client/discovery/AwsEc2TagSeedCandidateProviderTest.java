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

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider.AddressField;
import com.aerospike.client.discovery.StubAwsServer.Reply;
import com.aerospike.client.discovery.StubAwsServer.Request;

public class AwsEc2TagSeedCandidateProviderTest {
	static final String KEY = "AKIDEXAMPLE";
	static final String SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
	static final String TOKEN = "FwoGZXIvYXdzEXAMPLESESSIONTOKEN";
	private static final SeedCandidateContext INIT = new SeedCandidateContext(null, new Host[0], 0);
	private static final SeedCandidateContext TEND = new SeedCandidateContext(null, new Host[0], 7);
	private static final int MARGIN = 50;

	private final List<String> messages = new CopyOnWriteArrayList<>();
	private StubAwsServer stub;
	private TestAwsEnvironment env;

	@Before
	public void setUp() throws Exception {
		stub = new StubAwsServer();
		env = new TestAwsEnvironment(stub.url())
			.set("AWS_ACCESS_KEY_ID", KEY)
			.set("AWS_SECRET_ACCESS_KEY", SECRET)
			.set("AWS_SESSION_TOKEN", TOKEN);
		Log.setLevel(Log.Level.DEBUG);
		Log.setCallback((level, message) -> messages.add(level + " " + message));
	}

	@After
	public void tearDown() {
		stub.close();
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private AwsEc2TagSeedCandidateProvider.Builder builder() {
		return AwsEc2TagSeedCandidateProvider.builder().region("us-east-1").tagFilter("aerospike-cluster", "prod-a")
			.environment(env).timeout(5000);
	}

	private static String all() {
		return StubAwsServer.describe(null,
			StubAwsServer.instance("i-0b", "10.0.0.2", "198.51.100.2", "ip-10-0-0-2.ec2.internal",
				"ec2-198-51-100-2.compute-1.amazonaws.com"),
			StubAwsServer.instance("i-0a", "10.0.0.1", null, "ip-10-0-0-1.ec2.internal", ""),
			StubAwsServer.instance("i-0c", "10.0.0.3", "198.51.100.3", "ip-10-0-0-3.ec2.internal",
				"ec2-198-51-100-3.compute-1.amazonaws.com"),
			StubAwsServer.instance("i-0d", "10.0.0.1", null, "ip-10-0-0-1.ec2.internal", null));
	}

	private static List<String> names(List<Host> hosts) {
		List<String> list = new ArrayList<>();

		for (Host host : hosts) {
			list.add(host.name + ':' + host.port + (host.tlsName != null? '/' + host.tlsName : ""));
		}
		return list;
	}

	@Test
	public void eachAddressFieldAndMissingField() {
		Map<AddressField,List<String>> expected = new LinkedHashMap<>();
		expected.put(AddressField.PRIVATE_IP, Arrays.asList("10.0.0.1:3000", "10.0.0.2:3000", "10.0.0.3:3000"));
		expected.put(AddressField.PUBLIC_IP, Arrays.asList("198.51.100.2:3000", "198.51.100.3:3000"));
		expected.put(AddressField.PRIVATE_DNS, Arrays.asList("ip-10-0-0-1.ec2.internal:3000",
			"ip-10-0-0-2.ec2.internal:3000", "ip-10-0-0-3.ec2.internal:3000"));
		expected.put(AddressField.PUBLIC_DNS, Arrays.asList("ec2-198-51-100-2.compute-1.amazonaws.com:3000",
			"ec2-198-51-100-3.compute-1.amazonaws.com:3000"));
		stub.ec2Default(new Reply(200, all()));

		for (Map.Entry<AddressField,List<String>> entry : expected.entrySet()) {
			messages.clear();
			List<Host> hosts = builder().addressField(entry.getKey()).build().refreshSeedCandidates(TEND);
			System.out.println("AWS-TAGS-FIELD " + entry.getKey() + " " + names(hosts));
			assertEquals(entry.getValue(), names(hosts));

			boolean skips = entry.getKey() == AddressField.PUBLIC_IP || entry.getKey() == AddressField.PUBLIC_DNS;
			assertEquals(messages.toString(), skips, messages.stream().anyMatch(m ->
				m.equals("DEBUG AWS EC2 seed provider skipped 2 instances without " + entry.getKey().element)));
		}
	}

	@Test
	public void portAndTlsNameCarried() {
		stub.ec2Default(new Reply(200, all()));
		List<Host> hosts = builder().port(4333).tlsName("aerospike-tls").build().refreshSeedCandidates(TEND);
		System.out.println("AWS-TAGS-TLS " + names(hosts));

		assertEquals(Arrays.asList("10.0.0.1:4333/aerospike-tls", "10.0.0.2:4333/aerospike-tls",
			"10.0.0.3:4333/aerospike-tls"), names(hosts));
	}

	@Test
	public void filtersSentAsConfiguredWithRunningAddedAndSigned() {
		AwsEc2TagSeedCandidateProvider provider = builder()
			.filter("subnet-id", "subnet-1", "subnet-2")
			.build();
		provider.refreshSeedCandidates(TEND);

		assertEquals(1, stub.requests().size());
		Request request = stub.requests().get(0);
		System.out.println("AWS-TAGS-REQUEST " + request.method + ' ' + request.path + ' ' + request.params);

		Map<String,String> expected = new LinkedHashMap<>();
		expected.put("Action", "DescribeInstances");
		expected.put("Version", "2016-11-15");
		expected.put("Filter.1.Name", "tag:aerospike-cluster");
		expected.put("Filter.1.Value.1", "prod-a");
		expected.put("Filter.2.Name", "subnet-id");
		expected.put("Filter.2.Value.1", "subnet-1");
		expected.put("Filter.2.Value.2", "subnet-2");
		expected.put("Filter.3.Name", "instance-state-name");
		expected.put("Filter.3.Value.1", "running");
		assertEquals(expected, request.params);
		assertEquals("POST", request.method);
		assertEquals(TOKEN, request.headers.get("x-amz-security-token"));

		String date = request.headers.get("x-amz-date");
		String authorization = request.headers.get("authorization");
		System.out.println("AWS-TAGS-AUTHORIZATION " + authorization.replaceAll("Signature=[0-9a-f]+", "Signature=<64 hex>"));
		assertTrue(authorization, authorization.matches("AWS4-HMAC-SHA256 Credential=" + KEY + "/\\d{8}/us-east-1/ec2/" +
			"aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-security-token, Signature=[0-9a-f]{64}"));

		TreeMap<String,String> headers = new TreeMap<>();
		headers.put("content-type", request.headers.get("content-type"));
		headers.put("host", request.headers.get("host"));
		headers.put("x-amz-date", date);
		headers.put("x-amz-security-token", TOKEN);
		StringBuilder body = new StringBuilder();

		for (Map.Entry<String,String> entry : request.params.entrySet()) {
			body.append(body.length() > 0? "&" : "").append(AwsSigV4.encode(entry.getKey())).append('=')
				.append(AwsSigV4.encode(entry.getValue()));
		}
		assertEquals(AwsSigV4.authorization(KEY, SECRET, "POST", "/", "", headers,
			body.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8), "us-east-1", "ec2", date), authorization);
	}

	@Test
	public void paginationFollowsNextToken() {
		stub.ec2(new Reply(200, StubAwsServer.describe("page-2-token",
				StubAwsServer.instance("i-1", "10.0.0.1", null, null, null))),
			new Reply(200, StubAwsServer.describe("page-3-token+/=",
				StubAwsServer.instance("i-2", "10.0.0.2", null, null, null))),
			new Reply(200, StubAwsServer.describe(null,
				StubAwsServer.instance("i-3", "10.0.0.3", null, null, null))));

		List<Host> hosts = builder().build().refreshSeedCandidates(TEND);
		List<Request> requests = stub.requests();
		System.out.println("AWS-TAGS-PAGES " + requests.size() + " requests, NextToken " +
			requests.get(0).params.get("NextToken") + ", " + requests.get(1).params.get("NextToken") + ", " +
			requests.get(2).params.get("NextToken") + " hosts=" + names(hosts));

		assertEquals(3, requests.size());
		assertNull(requests.get(0).params.get("NextToken"));
		assertEquals("page-2-token", requests.get(1).params.get("NextToken"));
		assertEquals("page-3-token+/=", requests.get(2).params.get("NextToken"));
		assertEquals("instance-state-name", requests.get(2).params.get("Filter.2.Name"));
		assertEquals(Arrays.asList("10.0.0.1:3000", "10.0.0.2:3000", "10.0.0.3:3000"), names(hosts));
		assertTrue(messages.contains("DEBUG AWS EC2 seed provider us-east-1 returned " + hosts +
			" from 3 instances in 3 pages"));
	}

	@Test
	public void apiErrorThrowsWithCodeAndRequestIdAndNoSecret() {
		stub.ec2Default(new Reply(403, StubAwsServer.error("UnauthorizedOperation",
			"You are not authorized to perform this operation.", "ea966190-f9aa-478e-9ede-example")));

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> builder().build().refreshSeedCandidates(TEND));
		String authorization = stub.requests().get(0).headers.get("authorization");
		String signature = authorization.substring(authorization.lastIndexOf('=') + 1);
		System.out.println("AWS-TAGS-ERROR " + ae.getMessage());

		assertTrue(ae.getMessage(), ae.getMessage().contains("AWS EC2 seed provider: DescribeInstances failed: HTTP 403 " +
			"UnauthorizedOperation: You are not authorized to perform this operation. " +
			"(request id ea966190-f9aa-478e-9ede-example)"));
		assertNoSecret(ae.getMessage(), signature);
		assertNoSecret(messages.toString(), signature);
	}

	@Test
	public void errorEchoingSecretMaterialIsRedacted() {
		stub.ec2Default(new Reply(400, StubAwsServer.error("AuthFailure", "token " + TOKEN + " secret " + SECRET,
			"req-1")));

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> builder().build().refreshSeedCandidates(TEND));
		System.out.println("AWS-TAGS-REDACTED " + ae.getMessage());
		assertTrue(ae.getMessage(), ae.getMessage().contains("AuthFailure: token <redacted> secret <redacted> (request id req-1)"));
		assertNoSecret(ae.getMessage(), "");
	}

	@Test
	public void xmlWithDoctypeRejected() {
		stub.ec2Default(new Reply(200, "<?xml version=\"1.0\"?><!DOCTYPE r [<!ENTITY x SYSTEM \"file:///etc/passwd\">]>" +
			"<DescribeInstancesResponse><reservationSet>&x;</reservationSet></DescribeInstancesResponse>"));

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> builder().build().refreshSeedCandidates(TEND));
		System.out.println("AWS-TAGS-DOCTYPE " + ae.getMessage());
		assertTrue(ae.getMessage(), ae.getMessage().contains("invalid DescribeInstances response"));
		assertTrue(ae.getMessage(), ae.getMessage().contains("DOCTYPE"));
	}

	@Test
	public void unansweredStubReturnsWithinTimeoutAndInitTimeout() throws Exception {
		try (ServerSocket silent = StubAwsServer.silent()) {
			AwsEc2TagSeedCandidateProvider provider = builder().endpoint("http://127.0.0.1:" + silent.getLocalPort())
				.timeout(80).initTimeout(600).build();

			long elapsedInit = elapsed(() -> provider.refreshSeedCandidates(INIT), "timed out after 600ms");
			long elapsedTend = elapsed(() -> provider.refreshSeedCandidates(TEND), "timed out after 80ms");
			System.out.println("AWS-TAGS-UNANSWERED initTimeout=600ms elapsed=" + elapsedInit + "ms timeout=80ms elapsed=" +
				elapsedTend + "ms margin=" + MARGIN + "ms");

			assertTrue(elapsedInit + "ms", elapsedInit >= 600 && elapsedInit <= 600 + MARGIN);
			assertTrue(elapsedTend + "ms", elapsedTend >= 80 && elapsedTend <= 80 + MARGIN);
		}
	}

	@Test
	public void unansweredCredentialEndpointBoundedByTimeout() throws Exception {
		try (ServerSocket silent = StubAwsServer.silent()) {
			TestAwsEnvironment imds = new TestAwsEnvironment("http://127.0.0.1:" + silent.getLocalPort());
			AwsEc2TagSeedCandidateProvider provider = AwsEc2TagSeedCandidateProvider.builder().region("us-east-1")
				.tagFilter("aerospike-cluster", "prod-a").endpoint(stub.url()).timeout(80).environment(imds).build();

			long elapsed = elapsed(() -> provider.refreshSeedCandidates(TEND), "IMDS token timed out");
			System.out.println("AWS-TAGS-UNANSWERED-IMDS timeout=80ms elapsed=" + elapsed + "ms");
			assertTrue(elapsed + "ms", elapsed >= 80 && elapsed <= 80 + MARGIN);
		}
	}

	private static long elapsed(Runnable call, String message) {
		long begin = System.nanoTime();
		AerospikeException ae = assertThrows(AerospikeException.class, call::run);
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
		System.out.println("AWS-TAGS-TIMEOUT-MESSAGE " + ae.getMessage());
		assertTrue(ae.getMessage(), ae.getMessage().contains(message));
		return elapsed;
	}

	@Test
	public void noNewThread() throws Exception {
		stub.ec2Default(new Reply(200, all()));

		try (ServerSocket silent = StubAwsServer.silent()) {
			TestAwsEnvironment imds = new TestAwsEnvironment(stub.url());
			stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
			Set<Thread> before = threads();

			List<SeedCandidateProvider> providers = new ArrayList<>();
			providers.add(builder().build());
			providers.add(AwsEc2TagSeedCandidateProvider.builder().region("us-east-1").filter("tag:a", "b")
				.environment(imds).build());
			AwsEc2TagSeedCandidateProvider hung = builder().endpoint("http://127.0.0.1:" + silent.getLocalPort())
				.timeout(20).build();

			for (SeedCandidateProvider provider : providers) {
				provider.refreshSeedCandidates(INIT);

				for (int i = 1; i <= 5; i++) {
					assertEquals(3, provider.refreshSeedCandidates(new SeedCandidateContext(null, new Host[0], i)).size());
				}
			}

			for (int i = 0; i < 3; i++) {
				assertThrows(AerospikeException.class, () -> hung.refreshSeedCandidates(TEND));
			}

			Set<Thread> after = threads();
			Set<Thread> added = new HashSet<>(after);
			added.removeAll(before);
			System.out.println("AWS-TAGS-THREADS before=" + threadNames(before));
			System.out.println("AWS-TAGS-THREADS after=" + threadNames(after));
			System.out.println("AWS-TAGS-THREADS new=" + threadNames(added) + " refreshes=" + (2 * 6 + 3) +
				" imdsRequests=" + stub.count("/latest/api/token"));

			assertTrue(threadNames(added).toString(), added.isEmpty());
			assertEquals("imds", ((AwsEc2TagSeedCandidateProvider)providers.get(1)).credentialSource());
		}
	}

	private static Set<Thread> threads() {
		Set<Thread> set = new HashSet<>();

		for (Thread thread : Thread.getAllStackTraces().keySet()) {
			if (thread.isAlive()) {
				set.add(thread);
			}
		}
		return set;
	}

	private static Set<String> threadNames(Set<Thread> threads) {
		Set<String> names = new TreeSet<>();

		for (Thread thread : threads) {
			names.add(thread.getName());
		}
		return names;
	}

	@Test
	public void invalidConfigRejectedAtConstruction() {
		Map<String,Supplier<AwsEc2TagSeedCandidateProvider.Builder>> cases = new LinkedHashMap<>();
		cases.put("Invalid AWS EC2 seed provider region: 'null'",
			() -> AwsEc2TagSeedCandidateProvider.builder().tagFilter("a", "b"));
		cases.put("Invalid AWS EC2 seed provider region: 'US East'", () -> builder().region("US East"));
		cases.put("Invalid AWS EC2 seed provider filters: at least one filter is required",
			() -> AwsEc2TagSeedCandidateProvider.builder().region("us-east-1"));
		cases.put("Invalid AWS EC2 seed provider filter name: ''", () -> builder().filter("", "x"));
		cases.put("Invalid AWS EC2 seed provider filter 'subnet-id' values: empty", () -> builder().filter("subnet-id"));
		cases.put("Invalid AWS EC2 seed provider filter 'subnet-id' value: 'null'",
			() -> builder().filter("subnet-id", (String)null));
		cases.put("Invalid AWS EC2 seed provider filter name: 'instance-state-name': the provider adds instance-state-name=running",
			() -> builder().filter("instance-state-name", "stopped"));
		cases.put("Invalid AWS EC2 seed provider addressField: null", () -> builder().addressField(null));
		cases.put("Invalid AWS EC2 seed provider port: 0", () -> builder().port(0));
		cases.put("Invalid AWS EC2 seed provider port: 65536", () -> builder().port(65536));
		cases.put("Invalid AWS EC2 seed provider tlsName: empty", () -> builder().tlsName(""));
		cases.put("Invalid AWS EC2 seed provider timeout: 0", () -> builder().timeout(0));
		cases.put("Invalid AWS EC2 seed provider initTimeout: -1", () -> builder().initTimeout(-1));
		cases.put("Invalid AWS EC2 seed provider endpoint: 'ftp://ec2.example.com': expected http or https URL with a " +
			"host and no query", () -> builder().endpoint("ftp://ec2.example.com"));
		cases.put("Invalid AWS EC2 seed provider endpoint: 'http://ec2 example'", () -> builder().endpoint("http://ec2 example"));
		cases.put("Invalid AWS EC2 seed provider credentials: credentials and credentialsHook cannot both be set",
			() -> builder().credentials(KEY, SECRET, null).credentialsHook(() -> null));

		for (Map.Entry<String,Supplier<AwsEc2TagSeedCandidateProvider.Builder>> entry : cases.entrySet()) {
			AerospikeException ae = assertThrows(entry.getKey(), AerospikeException.class, () -> entry.getValue().get().build());
			System.out.println("AWS-TAGS-INVALID " + ae.getMessage());
			assertTrue(ae.getMessage(), ae.getMessage().contains(entry.getKey()));
		}

		AerospikeException ae = assertThrows(AerospikeException.class, () -> builder().credentials(KEY, "", null));
		assertTrue(ae.getMessage(), ae.getMessage().contains("secret access key is empty"));
	}

	@Test
	public void gettersAndDefaults() {
		AwsEc2TagSeedCandidateProvider provider = AwsEc2TagSeedCandidateProvider.builder().region("eu-west-1")
			.tagFilter("aerospike-cluster", "prod-a").build();

		assertEquals("eu-west-1", provider.getRegion());
		assertEquals(Collections.singletonMap("tag:aerospike-cluster", Collections.singletonList("prod-a")),
			provider.getFilters());
		assertEquals(AddressField.PRIVATE_IP, provider.getAddressField());
		assertEquals(3000, provider.getPort());
		assertNull(provider.getTlsName());
		assertEquals(80, provider.getTimeout());
		assertEquals(10000, provider.getInitTimeout());
		assertNull(provider.getEndpoint());
		assertTrue(provider.needsPeriodicRefresh());
		assertTrue(provider.supportsDeadline());
		assertEquals("aws-tags eu-west-1 {tag:aerospike-cluster=[prod-a]}", provider.toString());
		assertThrows(UnsupportedOperationException.class, () -> provider.getFilters().put("x", null));
		assertFalse(new AwsEc2TagSeedCandidateProvider.Credentials(KEY, SECRET, TOKEN).toString().contains(SECRET));
	}

	static void assertNoSecret(String text, String signature) {
		assertFalse(text, text.contains(SECRET));
		assertFalse(text, text.contains(TOKEN));

		if (! signature.isEmpty()) {
			assertFalse(text, text.contains(signature));
		}
	}
}
