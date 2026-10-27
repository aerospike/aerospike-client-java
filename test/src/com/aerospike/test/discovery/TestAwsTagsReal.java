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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider;
import com.aerospike.client.policy.ClientPolicy;

/**
 * {@link AwsEc2TagSeedCandidateProvider} against real AWS, run by test/discovery/run_aws_tags_real.sh
 * on an EC2 instance in the same region as a small self-managed Aerospike cluster whose nodes carry
 * the tag. The instance profile role allows only {@code ec2:DescribeInstances}. Each latency run is a
 * fresh JVM ({@link AwsTagsRealProbe}), so the cold call includes credential resolution through
 * IMDSv2. The environment arrives as system properties and environment variables:
 * <ul>
 * <li>awsreal.region, awsreal.tagKey, awsreal.tagValue: region and tag filter</li>
 * <li>awsreal.expectedNodes: number of tagged running cluster nodes</li>
 * <li>awsreal.excluded: comma-separated private IPs of untagged and stopped instances</li>
 * <li>awsreal.namespace: namespace of the cluster</li>
 * <li>awsreal.runs, awsreal.warm, awsreal.tendRefreshes: cold runs, warm refreshes per run and tend
 * refreshes counted by the join case</li>
 * <li>AWS_TAGS_REAL_DENIED_ACCESS_KEY_ID, AWS_TAGS_REAL_DENIED_SECRET_ACCESS_KEY,
 * AWS_TAGS_REAL_DENIED_SESSION_TOKEN: temporary credentials of a role with no policy</li>
 * </ul>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestAwsTagsReal {
	private static final String[] AWS_ENV = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_PROFILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"};
	private static final int RECOMMENDED_TIMEOUT = AwsEc2TagSeedCandidateProvider.DEFAULT_TIMEOUT;
	private static final List<String> messages = new CopyOnWriteArrayList<>();

	private static String region;
	private static String tagKey;
	private static String tagValue;
	private static int expectedNodes;
	private static List<String> excluded;
	private static String namespace;
	private static int runs;
	private static int warm;
	private static int tendRefreshes;

	@BeforeClass
	public static void init() {
		region = required("awsreal.region");
		tagKey = required("awsreal.tagKey");
		tagValue = required("awsreal.tagValue");
		expectedNodes = Integer.parseInt(required("awsreal.expectedNodes"));
		excluded = Arrays.asList(required("awsreal.excluded").split(","));
		namespace = required("awsreal.namespace");
		runs = Integer.parseInt(required("awsreal.runs"));
		warm = Integer.parseInt(required("awsreal.warm"));
		tendRefreshes = Integer.parseInt(required("awsreal.tendRefreshes"));
		requiredEnv("AWS_TAGS_REAL_DENIED_ACCESS_KEY_ID");
		requiredEnv("AWS_TAGS_REAL_DENIED_SECRET_ACCESS_KEY");
		requiredEnv("AWS_TAGS_REAL_DENIED_SESSION_TOKEN");

		for (String name : AWS_ENV) {
			if (System.getenv(name) != null) {
				throw new IllegalStateException(name + " is set; the instance profile must be the only credential source");
			}
		}

		Log.setLevel(Log.Level.DEBUG);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN || level == Log.Level.ERROR || message.startsWith("AWS EC2 seed provider ")) {
				messages.add(message);
			}
		});
	}

	@Test
	public void case1LatencyAndImdsCredentials() throws Exception {
		List<Long> cold = new ArrayList<>();
		List<Long> warmTimes = new ArrayList<>();
		int warmFailures = 0;

		for (int run = 1; run <= runs; run++) {
			List<String> lines = probe(null);
			String credentials = line(lines, "PROBE credentials ");
			String hosts = line(lines, "PROBE hosts ");
			System.out.println("AWS-REAL-RUN " + run + " credentials='" + credentials + "' hosts=" + hosts +
				" cold=" + line(lines, "PROBE coldMicros ") + "us");

			assertNotNull("no credentials line (AWS rejected the request?): " + lines, credentials);
			assertTrue(credentials, credentials.contains("credentials from imds"));
			assertSeedList(hosts);
			cold.add(Long.parseLong(line(lines, "PROBE coldMicros ")));

			for (String micros : line(lines, "PROBE warmMicros ").replaceAll("[\\[\\] ]", "").split(",")) {
				warmTimes.add(Long.parseLong(micros));
			}
			warmFailures += Integer.parseInt(line(lines, "PROBE warmFailures "));
		}

		System.out.println("AWS-REAL-LATENCY cold " + stats(cold) + " runs=" + cold.size());
		System.out.println("AWS-REAL-LATENCY warm " + stats(warmTimes) + " calls=" + warmTimes.size() + " failures=" +
			warmFailures);
		System.out.println("AWS-REAL-LATENCY reference discoveryTendRefreshDeadline=100ms recommendedTimeout=" +
			RECOMMENDED_TIMEOUT + "ms warmOverTimeout=" + count(warmTimes, RECOMMENDED_TIMEOUT) + " warmOverDeadline=" +
			count(warmTimes, 100));
	}

	@Test
	public void case2RoleWithoutPolicyRefused() throws Exception {
		String secret = System.getenv("AWS_TAGS_REAL_DENIED_SECRET_ACCESS_KEY");
		String token = System.getenv("AWS_TAGS_REAL_DENIED_SESSION_TOKEN");
		List<String> lines = probe(new String[] {
			System.getenv("AWS_TAGS_REAL_DENIED_ACCESS_KEY_ID"), secret, token});
		String error = line(lines, "PROBE error ");
		String credentials = line(lines, "PROBE credentials ");
		System.out.println("AWS-REAL-DENIED credentials='" + credentials + "' error='" + error + "'");

		assertNotNull("no error: " + lines, error);
		assertTrue(error, error.contains("UnauthorizedOperation") && error.contains("(request id "));
		assertFalse(error, error.contains(secret));
		assertFalse(error, error.contains(token));
	}

	@Test
	public void case3ClientJoinsEveryTaggedNode() throws Exception {
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryRefreshInterval = policy.tendInterval;
		policy.seedCandidateProvider = AwsEc2TagSeedCandidateProvider.builder().region(region).tagFilter(tagKey, tagValue)
			.build();

		try (AerospikeClient client = new AerospikeClient(policy)) {
			long deadline = System.currentTimeMillis() + 60000;

			while (client.getNodes().length < expectedNodes || ownedPartitions(client) < Node.PARTITIONS) {
				if (System.currentTimeMillis() > deadline) {
					fail("nodes=" + client.getNodes().length + " owned=" + ownedPartitions(client) + " " + messages);
				}
				Thread.sleep(100);
			}

			messages.clear();
			long wait = (long)tendRefreshes * policy.discoveryRefreshInterval + 60000;
			deadline = System.currentTimeMillis() + wait;

			while (count(" returned ") < tendRefreshes && count("Seed candidate provider exceeded") == 0) {
				if (System.currentTimeMillis() > deadline) {
					fail("only " + count(" returned ") + " refreshes in " + wait + "ms: " + messages);
				}
				Thread.sleep(100);
			}

			System.out.println("AWS-REAL-JOIN nodes=" + client.getNodes().length + " expected=" + expectedNodes +
				" ownedPartitions=" + ownedPartitions(client) + '/' + Node.PARTITIONS + " tendRefreshes=" +
				count(" returned ") + " overruns=" + count("exceeding tend deadline") + " failures=" +
				count("Seed candidate refresh failed") + " tripped=" + (count("Seed candidate provider exceeded") > 0));
			assertEquals(expectedNodes, client.getNodes().length);
			assertEquals(0, count("Seed candidate provider exceeded"));
		}
	}

	private static void assertSeedList(String hosts) {
		assertNotNull(hosts);

		for (String address : excluded) {
			assertFalse(address + " in " + hosts, hosts.contains(address.trim() + ' '));
		}
		int n = hosts.equals("[]")? 0 : hosts.split(",").length;
		assertEquals(hosts, expectedNodes, n);
	}

	private static List<String> probe(String[] credentials) throws Exception {
		List<String> command = new ArrayList<>();
		command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
		command.add("-cp");
		command.add(System.getProperty("java.class.path"));
		command.add(AwsTagsRealProbe.class.getName());
		command.add(region);
		command.add(tagKey);
		command.add(tagValue);
		command.add(Integer.toString(warm));

		ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true);
		Map<String,String> env = pb.environment();

		if (credentials != null) {
			env.put("AWS_ACCESS_KEY_ID", credentials[0]);
			env.put("AWS_SECRET_ACCESS_KEY", credentials[1]);
			env.put("AWS_SESSION_TOKEN", credentials[2]);
		}

		Process process = pb.start();
		String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

		if (! process.waitFor(120, TimeUnit.SECONDS)) {
			process.destroyForcibly();
			fail("probe did not exit: " + output);
		}
		return Arrays.asList(output.split("\n"));
	}

	private static String line(List<String> lines, String prefix) {
		for (String line : lines) {
			if (line.startsWith(prefix)) {
				return line.substring(prefix.length()).trim();
			}
		}
		return null;
	}

	private static String stats(List<Long> micros) {
		List<Long> sorted = new ArrayList<>(micros);
		Collections.sort(sorted);
		return String.format("min=%.1fms median=%.1fms p99=%.1fms max=%.1fms", sorted.get(0) / 1000.0,
			sorted.get(sorted.size() / 2) / 1000.0, sorted.get(Math.min(sorted.size() - 1,
			(int)Math.ceil(sorted.size() * 0.99) - 1)) / 1000.0, sorted.get(sorted.size() - 1) / 1000.0);
	}

	private static int count(List<Long> micros, int ms) {
		int n = 0;

		for (long m : micros) {
			if (m > ms * 1000L) {
				n++;
			}
		}
		return n;
	}

	private static int count(String text) {
		int n = 0;

		for (String m : messages) {
			if (m.contains(text)) {
				n++;
			}
		}
		return n;
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

	private static String required(String name) {
		String value = System.getProperty(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("System property " + name + " is not set. Run test/discovery/run_aws_tags_real.sh");
		}
		return value;
	}

	private static String requiredEnv(String name) {
		String value = System.getenv(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("Environment variable " + name + " is not set. Run test/discovery/run_aws_tags_real.sh");
		}
		return value;
	}
}
