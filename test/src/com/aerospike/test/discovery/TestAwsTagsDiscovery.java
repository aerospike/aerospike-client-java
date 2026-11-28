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
package com.aerospike.test.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.yaml.snakeyaml.Yaml;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider;
import com.aerospike.client.discovery.DiscoveryExecutionMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.util.TestEnv;

/**
 * {@link AwsEc2TagSeedCandidateProvider} against the two node cluster and stub EC2 Query API
 * endpoint of the aerospike-client-conformance aws-tags environment. The client gets no static
 * seed: every seed comes from the stub. Credentials come from the environment variables the values
 * file sets to fixed fake values, so the environment source of the credential chain runs end to
 * end. The stub returns each node as a running instance whose private IP is the node's published
 * address, reachable from this host: node A on 127.0.0.1 and node B on another host address, both
 * on the same port. Each node advertises a private address as its alternate access address, which
 * {@link ClientPolicy#ipMap} maps to its published address. The seed list is read from the
 * provider's debug log line. aws-tags runs only in execution mode thread; every case states its
 * mode. The environment arrives as the environment variables of its values file:
 * <ul>
 * <li>EC2_ENDPOINT: the stub EC2 endpoint</li>
 * <li>EC2_STUB_CONTROL: the stub's control API, which sets its mode and delay and returns the
 * requests it received</li>
 * <li>AWS_REGION: region the provider signs for</li>
 * <li>ENV_NAMESPACE: namespace, replication factor 1</li>
 * <li>NODE_A_PORT: port node A and node B are reachable on</li>
 * <li>NODE_A_NAME, NODE_A_HOST, NODE_A_ADVERTISED_HOST, NODE_B_NAME, NODE_B_HOST,
 * NODE_B_ADVERTISED_HOST: node names, reachable addresses and advertised private addresses</li>
 * <li>CLIENT_CONFIG: YAML config file selecting provider aws-tags with the stub endpoint</li>
 * </ul>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestAwsTagsDiscovery {
	private static final String CONFIG_SYS_PROP = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";
	private static final String TAG_KEY = "aerospike-cluster";
	private static final String TAG_VALUE = "prod-a";
	private static final String RETURNED = "AWS EC2 seed provider ";
	private static final Pattern SEEDS = Pattern.compile("AWS EC2 seed provider \\S+ returned (\\[.*\\]) from (\\d+) " +
		"instances in (\\d+) pages");
	private static final Pattern TOOK = Pattern.compile("Seed candidate refresh took (\\d+)ms");
	private static final String FAILED = "Seed candidate refresh failed: ";
	private static final String ERROR_REQUEST_ID = "ea966190-f9aa-478e-9ede-example";
	private static final int REFRESH_INTERVAL = 1000;
	private static final int TIMEOUT = 50;
	private static final int COLD_CALL_MS = 700;
	private static final int TRIP_OVERRUNS = 3;
	private static final int MARGIN = 50;
	private static final int WAIT_MS = 30000;
	private static final int KEY_COUNT = 100;
	private static final List<String> messages = new CopyOnWriteArrayList<>();

	private static final HttpClient http = HttpClient.newHttpClient();

	private static String stubControl;
	private static String endpoint;
	private static String region;
	private static String namespace;
	private static int port;
	private static String nodeA;
	private static String hostA;
	private static String privateA;
	private static String nodeB;
	private static String hostB;
	private static String privateB;
	private static String config;
	private static String secret;
	private static String sessionToken;

	@BeforeClass
	public static void init() {
		if (System.getenv("AEROSPIKE_CLIENT_CONFIG_URL") != null) {
			throw new IllegalStateException("AEROSPIKE_CLIENT_CONFIG_URL is set and would override the test config");
		}
		endpoint = TestEnv.get("EC2_ENDPOINT");
		stubControl = TestEnv.get("EC2_STUB_CONTROL");
		region = TestEnv.get("AWS_REGION");
		namespace = TestEnv.get("ENV_NAMESPACE");
		port = TestEnv.getInt("NODE_A_PORT");
		nodeA = TestEnv.get("NODE_A_NAME");
		hostA = TestEnv.get("NODE_A_HOST");
		privateA = TestEnv.get("NODE_A_ADVERTISED_HOST");
		nodeB = TestEnv.get("NODE_B_NAME");
		hostB = TestEnv.get("NODE_B_HOST");
		privateB = TestEnv.get("NODE_B_ADVERTISED_HOST");
		config = TestEnv.get("CLIENT_CONFIG");
		secret = TestEnv.get("AWS_SECRET_ACCESS_KEY");
		sessionToken = TestEnv.get("AWS_SESSION_TOKEN");

		Log.setLevel(Log.Level.DEBUG);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN || level == Log.Level.ERROR || message.startsWith(RETURNED)) {
				messages.add(message);
				System.out.println("CLIENT-LOG " + level + ' ' + message.replace('\n', ' '));
			}
		});
	}

	@Before
	public void reset() throws Exception {
		messages.clear();
		stub("reset", "");
	}

	@After
	public void clearConfig() throws Exception {
		System.clearProperty(CONFIG_SYS_PROP);
		stub("settings", "{\"mode\": \"answer\"}");
	}

	@Test
	public void case1BothNodesJoinThroughProvider() throws Exception {
		try (AerospikeClient client = new AerospikeClient(policy(provider(AwsEc2TagSeedCandidateProvider.DEFAULT_TIMEOUT)))) {
			String seeds = assertJoins("join", client);
			assertEquals(seedList(hostA, hostB), seeds);

			List<Request> requests = requests();
			Request request = requests.get(0);
			System.out.println("AWS-TAGS-RESULT join requests=" + requests.size() + " params=" + request.params +
				" authorization=" + request.authorization.replaceAll("Signature=[0-9a-f]+", "Signature=<64 hex>") +
				" wellFormed=" + request.authorizationValid);

			Map<String,String> expected = new LinkedHashMap<>();
			expected.put("Action", "DescribeInstances");
			expected.put("Version", "2016-11-15");
			expected.put("Filter.1.Name", "tag:" + TAG_KEY);
			expected.put("Filter.1.Value.1", TAG_VALUE);
			expected.put("Filter.2.Name", "instance-state-name");
			expected.put("Filter.2.Value.1", "running");

			for (Request r : requests) {
				assertEquals(expected, r.params);
				assertTrue(r.authorization, r.authorizationValid);
			}
		}
	}

	@Test
	public void case4UnauthorizedOperationKeepsClientRunning() throws Exception {
		try (AerospikeClient client = new AerospikeClient(policy(provider(AwsEc2TagSeedCandidateProvider.DEFAULT_TIMEOUT)))) {
			assertJoins("unauthorized-before", client);
			messages.clear();
			int sent = requests().size();
			stub("settings", "{\"mode\": \"unauthorized\"}");

			String failure = await(() -> messages.stream().filter(m -> m.startsWith("Seed candidate refresh failed: "))
				.findFirst().orElse(null));
			String signature = requests().get(sent).signature;
			System.out.println("AWS-TAGS-WARN " + failure.trim());

			assertTrue(failure, failure.contains("DescribeInstances failed: HTTP 403 UnauthorizedOperation: You are not " +
				"authorized to perform this operation. (request id " + ERROR_REQUEST_ID + ")"));
			assertNoSecret(failure, signature);

			List<String> names = nodeNames(client);
			System.out.println("AWS-TAGS-RESULT unauthorized nodes=" + names.size() + ' ' + names + " connected=" +
				client.isConnected() + " secretInMessage=false signatureInMessage=false");
			assertEquals(List.of(nodeA, nodeB), names);
			assertKeysWork("unauthorized", client);
		}
	}

	@Test
	public void case5UnansweredStubOverrunsWithoutTrip() throws Exception {
		ClientPolicy policy = policy(provider(TIMEOUT));
		policy.discoveryRefreshTimeout = TIMEOUT;

		try (AerospikeClient client = new AerospikeClient(policy)) {
			assertHangOverruns("hang", client);
		}
	}

	@Test
	public void case6YamlUnansweredStubOverrunsWithoutTrip() throws Exception {
		System.setProperty(CONFIG_SYS_PROP, config);

		try (AerospikeClient client = new AerospikeClient(new ClientPolicy())) {
			assertNotNull(client.getConfigProvider());
			ClientPolicy merged = client.getClientPolicy();
			AwsEc2TagSeedCandidateProvider provider = (AwsEc2TagSeedCandidateProvider)merged.seedCandidateProvider;
			System.out.println("AWS-TAGS-RESULT yaml provider region=" + provider.getRegion() + " filters=" +
				provider.getFilters() + " port=" + provider.getPort() + " timeout=" + provider.getTimeout() + " endpoint=" +
				provider.getEndpoint() + " executionMode=" + merged.discoveryExecutionMode + " refreshTimeout=" +
				merged.discoveryRefreshTimeout + " ipMap=" + merged.ipMap);
			assertEquals(endpoint, provider.getEndpoint());
			assertEquals(TIMEOUT, provider.getTimeout());
			assertEquals(DiscoveryExecutionMode.THREAD, merged.discoveryExecutionMode);
			assertEquals(TIMEOUT, merged.discoveryRefreshTimeout);
			assertHangOverruns("yaml-hang", client);
		}
	}

	@Test
	public void case7ColdCallsWorkWithDefaultTimeouts() throws Exception {
		stub("settings", "{\"delay_ms\": " + COLD_CALL_MS + "}");
		AwsEc2TagSeedCandidateProvider provider = AwsEc2TagSeedCandidateProvider.builder()
			.region(region)
			.tagFilter(TAG_KEY, TAG_VALUE)
			.port(port)
			.endpoint(endpoint)
			.build();
		ClientPolicy policy = policy(provider);
		long begin = System.currentTimeMillis();

		try (AerospikeClient client = new AerospikeClient(policy)) {
			long constructed = System.currentTimeMillis() - begin;
			assertJoins("cold", client);
			await(() -> (messages.stream().filter(m -> seeds(m) != null).count() >= 3)? Boolean.TRUE : null);

			long returned = messages.stream().filter(m -> seeds(m) != null).count();
			List<String> refreshWarnings = messages.stream().filter(m -> m.startsWith("Seed candidate "))
				.collect(Collectors.toList());
			System.out.println("AWS-TAGS-RESULT cold delayMs=" + COLD_CALL_MS + " constructedMs=" + constructed +
				" timeout=" + provider.getTimeout() + " refreshTimeout=" + policy.discoveryRefreshTimeout +
				" refreshesReturned=" + returned + " refreshWarnings=" + refreshWarnings);
			assertEquals(AwsEc2TagSeedCandidateProvider.DEFAULT_TIMEOUT, provider.getTimeout());
			assertEquals(new ClientPolicy().discoveryRefreshTimeout, policy.discoveryRefreshTimeout);
			assertTrue(constructed + "ms", constructed >= COLD_CALL_MS);
			assertEquals(List.of(), refreshWarnings);
			assertKeysWork("cold", client);
		}
	}

	@Test
	public void case8TendModeFailsConstruction() throws Exception {
		ClientPolicy policy = policy(provider(80));
		policy.discoveryExecutionMode = DiscoveryExecutionMode.TEND;

		AerospikeException ae = assertThrows(AerospikeException.class, () -> new AerospikeClient(policy).close());
		System.out.println("AWS-TAGS-RESULT tend " + ae.getMessage() + " requests=" + requests().size());
		assertTrue(ae.getMessage(), ae.getMessage().contains("must support a deadline in discovery execution mode tend"));
		assertEquals(0, requests().size());
	}

	private static void assertHangOverruns(String name, AerospikeClient client) throws Exception {
		assertJoins(name + "-before", client);
		messages.clear();
		stub("settings", "{\"mode\": \"hang\"}");

		await(() -> messages.stream().anyMatch(m -> m.startsWith(FAILED))? Boolean.TRUE : null);
		Host[] lastGood = ClusterInternals.seeds(client);

		await(() -> (messages.stream().filter(m -> m.startsWith(FAILED)).count() > TRIP_OVERRUNS &&
			messages.stream().anyMatch(m -> TOOK.matcher(m).find()))? Boolean.TRUE : null);
		int failures = 0;
		int overruns = 0;

		for (String m : messages) {
			System.out.println("AWS-TAGS-WARN " + m.trim());

			if (m.startsWith(FAILED)) {
				failures++;
				assertTrue(m, m.contains("AWS EC2 seed provider: DescribeInstances timed out after " + TIMEOUT + "ms"));
			}

			Matcher matcher = TOOK.matcher(m);

			if (matcher.find()) {
				overruns++;
				int took = Integer.parseInt(matcher.group(1));
				assertTrue(m, took > TIMEOUT && took <= TIMEOUT + MARGIN);
				assertTrue(m, m.contains("exceeding refresh timeout " + TIMEOUT + "ms"));
			}
		}

		Host[] seeds = ClusterInternals.seeds(client);
		List<String> names = nodeNames(client);
		System.out.println("AWS-TAGS-RESULT " + name + " refreshFailures=" + failures + " overrunWarnings=" + overruns +
			" seeds=" + List.of(seeds) + " lastGoodKept=" + (seeds == lastGood) + " nodes=" + names.size() + ' ' + names +
			" connected=" + client.isConnected());
		assertTrue(messages.stream().noneMatch(m -> m.startsWith("Seed candidate provider exceeded tend deadline")));
		assertTrue(failures > TRIP_OVERRUNS);
		assertTrue(overruns >= 1);
		assertSame(lastGood, seeds);
		assertEquals(seedList(hostA, hostB), List.of(seeds).toString());
		assertEquals(List.of(nodeA, nodeB), names);
		assertKeysWork(name, client);
	}

	private static AwsEc2TagSeedCandidateProvider provider(int timeout) {
		return AwsEc2TagSeedCandidateProvider.builder()
			.region(region)
			.tagFilter(TAG_KEY, TAG_VALUE)
			.port(port)
			.timeout(timeout)
			.endpoint(endpoint)
			.build();
	}

	private static ClientPolicy policy(AwsEc2TagSeedCandidateProvider provider) {
		ClientPolicy policy = new ClientPolicy();
		policy.useServicesAlternate = true;
		policy.ipMap = new HashMap<>();
		policy.ipMap.put(privateA, hostA);
		policy.ipMap.put(privateB, hostB);
		policy.discoveryExecutionMode = DiscoveryExecutionMode.THREAD;
		policy.discoveryRefreshInterval = REFRESH_INTERVAL;
		policy.seedCandidateProvider = provider;
		return policy;
	}

	private static String assertJoins(String name, AerospikeClient client) throws Exception {
		String seeds = messages.stream().map(TestAwsTagsDiscovery::seeds).filter(s -> s != null).findFirst().orElse(null);
		assertNotNull("no seed list logged: " + messages, seeds);

		await(() -> (client.getNodes().length == 2 && ownedPartitions(client) == Node.PARTITIONS)? Boolean.TRUE : null);
		List<String> names = nodeNames(client);
		int owned = ownedPartitions(client);

		System.out.println("AWS-TAGS-RESULT " + name + " seeds=" + seeds + " nodes=" + names.size() + ' ' + names +
			" ownedPartitions=" + owned + '/' + Node.PARTITIONS);
		assertEquals(List.of(nodeA, nodeB), names);
		assertEquals(Node.PARTITIONS, owned);
		assertKeysWork(name, client);
		return seeds;
	}

	private static String seeds(String message) {
		Matcher m = SEEDS.matcher(message);
		return m.matches()? m.group(1) : null;
	}

	private static String seedList(String... hosts) {
		List<String> list = new ArrayList<>();

		for (String host : hosts) {
			list.add(host);
		}
		list.sort(null);

		StringBuilder sb = new StringBuilder("[");

		for (String host : list) {
			sb.append(sb.length() > 1? ", " : "").append(host).append(' ').append(port);
		}
		return sb.append(']').toString();
	}

	private static void assertNoSecret(String text, String signature) {
		assertFalse(text, text.contains(secret));
		assertFalse(text, text.contains(sessionToken));
		assertFalse(text, text.contains(signature));
	}

	private static void assertKeysWork(String name, AerospikeClient client) {
		for (int i = 0; i < KEY_COUNT; i++) {
			Key key = new Key(namespace, "aws-tags-discovery", i);
			client.put(null, key, new Bin("v", i));
			Record record = client.get(null, key);
			assertEquals(i, record.getInt("v"));
		}
		System.out.println("AWS-TAGS-RESULT " + name + " keys=" + KEY_COUNT + " written and read");
	}

	private static int ownedPartitions(AerospikeClient client) {
		Partitions partitions = client.getCluster().partitionMap.get(namespace);

		if (partitions == null) {
			return 0;
		}

		int owned = 0;

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = partitions.replicas[0].get(i);

			if (node != null && node.isActive()) {
				owned++;
			}
		}
		return owned;
	}

	private static List<String> nodeNames(AerospikeClient client) {
		List<String> names = new ArrayList<>();

		for (Node node : client.getNodes()) {
			names.add(node.getName());
		}
		names.sort(null);
		return names;
	}

	private static <T> T await(Supplier<T> condition) throws Exception {
		long deadline = System.currentTimeMillis() + WAIT_MS;
		T value;

		while ((value = condition.get()) == null) {
			if (System.currentTimeMillis() > deadline) {
				fail("condition not met within " + WAIT_MS + "ms: " + messages);
			}
			Thread.sleep(50);
		}
		return value;
	}

	private static List<Request> requests() throws Exception {
		List<Map<String,Object>> list = new Yaml().load(stub("requests", null));
		List<Request> requests = new ArrayList<>();

		for (Map<String,Object> r : list) {
			requests.add(new Request(r));
		}
		return requests;
	}

	private static String stub(String path, String body) throws Exception {
		HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(stubControl + '/' + path));
		HttpRequest request = (body == null)? builder.GET().build() :
			builder.POST(HttpRequest.BodyPublishers.ofString(body)).build();
		HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());

		if (response.statusCode() != 200) {
			throw new IllegalStateException("Stub EC2 control " + path + " failed: HTTP " + response.statusCode() + ' ' +
				response.body());
		}
		return response.body();
	}

	private static final class Request {
		final Map<String,String> params;
		final String authorization;
		final boolean authorizationValid;
		final String signature;

		@SuppressWarnings("unchecked")
		Request(Map<String,Object> request) {
			this.params = (Map<String,String>)request.get("params");
			this.authorization = (String)request.get("authorization");
			this.authorizationValid = (Boolean)request.get("authorization_valid");
			this.signature = (String)request.get("signature");
		}
	}
}
