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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.discovery.StubEc2Server.Mode;

/**
 * {@link AwsEc2TagSeedCandidateProvider} against the two node cluster started by
 * test/discovery/run_aws_tags.sh and a stub EC2 Query API endpoint served from this JVM. The client
 * gets no static seed: every seed comes from the stub. Credentials come from the environment
 * variables the driver sets to fixed fake values, so the environment source of the credential chain
 * runs end to end. The stub returns each node as a running instance whose private IP is the node's
 * published address, reachable from this host: node A on 127.0.0.1 and node B on another host
 * address, both on the same port. Each node advertises a private address as its alternate access
 * address, which {@link ClientPolicy#ipMap} maps to its published address. The seed list is read
 * from the provider's debug log line. The environment arrives as system properties:
 * <ul>
 * <li>aws.stubPort: port of the stub EC2 endpoint, on 127.0.0.1</li>
 * <li>aws.region: region the provider signs for</li>
 * <li>aws.namespace: namespace, replication factor 1</li>
 * <li>aws.port: port node A and node B are reachable on</li>
 * <li>aws.nodeA, aws.hostA, aws.privateA, aws.nodeB, aws.hostB, aws.privateB: node names, reachable
 * addresses and advertised private addresses</li>
 * <li>aws.config: YAML config file selecting provider aws-tags with the stub endpoint</li>
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
	private static final int REFRESH_INTERVAL = 1000;
	private static final int DEADLINE = 50;
	private static final int MARGIN = 50;
	private static final int WAIT_MS = 30000;
	private static final int KEY_COUNT = 100;
	private static final List<String> messages = new CopyOnWriteArrayList<>();

	private static StubEc2Server stub;
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
	private static String accessKey;
	private static String secret;
	private static String sessionToken;

	@BeforeClass
	public static void init() throws Exception {
		if (System.getenv("AEROSPIKE_CLIENT_CONFIG_URL") != null) {
			throw new IllegalStateException("AEROSPIKE_CLIENT_CONFIG_URL is set and would override the test config");
		}
		int stubPort = Integer.parseInt(required("aws.stubPort"));
		region = required("aws.region");
		namespace = required("aws.namespace");
		port = Integer.parseInt(required("aws.port"));
		nodeA = required("aws.nodeA");
		hostA = required("aws.hostA");
		privateA = required("aws.privateA");
		nodeB = required("aws.nodeB");
		hostB = required("aws.hostB");
		privateB = required("aws.privateB");
		config = required("aws.config");
		accessKey = requiredEnv("AWS_ACCESS_KEY_ID");
		secret = requiredEnv("AWS_SECRET_ACCESS_KEY");
		sessionToken = requiredEnv("AWS_SESSION_TOKEN");
		endpoint = "http://127.0.0.1:" + stubPort;
		stub = new StubEc2Server(stubPort, accessKey, region);

		Log.setLevel(Log.Level.DEBUG);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN || level == Log.Level.ERROR || message.startsWith(RETURNED)) {
				messages.add(message);
				System.out.println("CLIENT-LOG " + level + ' ' + message.replace('\n', ' '));
			}
		});
	}

	@AfterClass
	public static void destroy() {
		if (stub != null) {
			stub.close();
		}
	}

	@Before
	public void reset() {
		messages.clear();
		stub.reset();
		stub.instance("i-0a1b2c3d4e5f60001", hostA);
		stub.instance("i-0a1b2c3d4e5f60002", hostB);
	}

	@After
	public void clearConfig() {
		System.clearProperty(CONFIG_SYS_PROP);
		stub.mode(Mode.ANSWER);
	}

	@Test
	public void case1BothNodesJoinThroughProvider() throws Exception {
		try (AerospikeClient client = new AerospikeClient(policy(provider(80)))) {
			String seeds = assertJoins("join", client);
			assertEquals(seedList(hostA, hostB), seeds);

			StubEc2Server.Request request = stub.requests().get(0);
			System.out.println("AWS-TAGS-RESULT join requests=" + stub.requests().size() + " params=" + request.params +
				" authorization=" + request.authorization.replaceAll("Signature=[0-9a-f]+", "Signature=<64 hex>") +
				" wellFormed=" + request.authorizationValid);

			Map<String,String> expected = new LinkedHashMap<>();
			expected.put("Action", "DescribeInstances");
			expected.put("Version", "2016-11-15");
			expected.put("Filter.1.Name", "tag:" + TAG_KEY);
			expected.put("Filter.1.Value.1", TAG_VALUE);
			expected.put("Filter.2.Name", "instance-state-name");
			expected.put("Filter.2.Value.1", "running");

			for (StubEc2Server.Request r : stub.requests()) {
				assertEquals(expected, r.params);
				assertTrue(r.authorization, r.authorizationValid);
			}
		}
	}

	@Test
	public void case4UnauthorizedOperationKeepsClientRunning() throws Exception {
		try (AerospikeClient client = new AerospikeClient(policy(provider(80)))) {
			assertJoins("unauthorized-before", client);
			messages.clear();
			int sent = stub.requests().size();
			stub.mode(Mode.UNAUTHORIZED);

			String failure = await(() -> messages.stream().filter(m -> m.startsWith("Seed candidate refresh failed: "))
				.findFirst().orElse(null));
			String signature = stub.requests().get(sent).signature();
			System.out.println("AWS-TAGS-WARN " + failure.trim());

			assertTrue(failure, failure.contains("DescribeInstances failed: HTTP 403 UnauthorizedOperation: You are not " +
				"authorized to perform this operation. (request id " + StubEc2Server.ERROR_REQUEST_ID + ")"));
			assertNoSecret(failure, signature);

			List<String> names = nodeNames(client);
			System.out.println("AWS-TAGS-RESULT unauthorized nodes=" + names.size() + ' ' + names + " connected=" +
				client.isConnected() + " secretInMessage=false signatureInMessage=false");
			assertEquals(List.of(nodeA, nodeB), names);
			assertKeysWork("unauthorized", client);
		}
	}

	@Test
	public void case5UnansweredStubTripsProvider() throws Exception {
		ClientPolicy policy = policy(provider(DEADLINE));
		policy.discoveryTendRefreshDeadline = DEADLINE;

		try (AerospikeClient client = new AerospikeClient(policy)) {
			assertHangTrips("hang", client);
		}
	}

	@Test
	public void case6YamlUnansweredStubTripsProvider() throws Exception {
		System.setProperty(CONFIG_SYS_PROP, config);

		try (AerospikeClient client = new AerospikeClient(new ClientPolicy())) {
			assertNotNull(client.getConfigProvider());
			ClientPolicy merged = client.getClientPolicy();
			AwsEc2TagSeedCandidateProvider provider = (AwsEc2TagSeedCandidateProvider)merged.seedCandidateProvider;
			System.out.println("AWS-TAGS-RESULT yaml provider region=" + provider.getRegion() + " filters=" +
				provider.getFilters() + " port=" + provider.getPort() + " timeout=" + provider.getTimeout() + " endpoint=" +
				provider.getEndpoint() + " deadline=" + merged.discoveryTendRefreshDeadline + " ipMap=" + merged.ipMap);
			assertEquals(endpoint, provider.getEndpoint());
			assertEquals(DEADLINE, provider.getTimeout());
			assertEquals(DEADLINE, merged.discoveryTendRefreshDeadline);
			assertHangTrips("yaml-hang", client);
		}
	}

	private static void assertHangTrips(String name, AerospikeClient client) throws Exception {
		assertJoins(name + "-before", client);
		messages.clear();
		stub.mode(Mode.HANG);

		String trip = await(() -> messages.stream().filter(m -> m.startsWith("Seed candidate provider exceeded tend deadline"))
			.findFirst().orElse(null));
		int failures = 0;
		int overruns = 0;

		for (String m : messages) {
			System.out.println("AWS-TAGS-WARN " + m.trim());

			if (m.startsWith("Seed candidate refresh failed: ")) {
				failures++;
				assertTrue(m, m.contains("AWS EC2 seed provider: DescribeInstances timed out after " + DEADLINE + "ms"));
			}

			Matcher matcher = TOOK.matcher(m);

			if (matcher.find()) {
				overruns++;
				int took = Integer.parseInt(matcher.group(1));
				assertTrue(m, took > DEADLINE && took <= DEADLINE + MARGIN);
			}
		}

		List<String> names = nodeNames(client);
		System.out.println("AWS-TAGS-RESULT " + name + " refreshFailures=" + failures + " overrunWarnings=" + overruns +
			" trip='" + trip + "' nodes=" + names.size() + ' ' + names + " connected=" + client.isConnected());
		assertTrue(trip, trip.startsWith("Seed candidate provider exceeded tend deadline 3 consecutive times"));
		assertTrue(overruns >= 3);
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

	private static String required(String name) {
		String value = System.getProperty(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("System property " + name + " is not set. Run test/discovery/run_aws_tags.sh");
		}
		return value;
	}

	private static String requiredEnv(String name) {
		String value = System.getenv(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("Environment variable " + name + " is not set. Run test/discovery/run_aws_tags.sh");
		}
		return value;
	}
}
