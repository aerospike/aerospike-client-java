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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider;
import com.aerospike.client.discovery.SeedCandidateContext;
import com.aerospike.client.policy.ClientPolicy;

/**
 * The Java probe for the aws-tags real AWS harness in aerospike-client-conformance (aws-real/).
 * Runs {@link AwsEc2TagSeedCandidateProvider} against real AWS in one of three modes and prints one
 * JSON object on stdout, as that harness's README describes:
 * <ul>
 * <li>{@code latency}: a cold first refresh, credentials included, then AWS_REAL_WARM warm refreshes</li>
 * <li>{@code denied}: one refresh with the credentials in the environment</li>
 * <li>{@code join}: a client seeded only by the provider joins the tagged nodes, then
 * AWS_REAL_REFRESHES tend refreshes are counted</li>
 * </ul>
 * Environment: AWS_REAL_REGION, AWS_REAL_TAG (key=value), and optionally AWS_REAL_WARM (default 20),
 * AWS_REAL_EXPECTED_NODES (join), AWS_REAL_NAMESPACE (default test), AWS_REAL_REFRESHES (default 30).
 */
public final class AwsTagsRealProbe {
	private static final List<String> messages = new CopyOnWriteArrayList<>();

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("usage: AwsTagsRealProbe latency|denied|join");
			System.exit(2);
		}

		String tag = env("AWS_REAL_TAG", null);
		int eq = tag.indexOf('=');

		if (eq <= 0 || eq == tag.length() - 1) {
			System.err.println("AWS_REAL_TAG must be key=value");
			System.exit(2);
		}

		String region = env("AWS_REAL_REGION", null);
		String tagKey = tag.substring(0, eq);
		String tagValue = tag.substring(eq + 1);

		switch (args[0]) {
		case "latency":
			Log.setLevel(Log.Level.INFO);
			Log.setCallback((level, message) -> messages.add(message));
			System.out.println(latency(provider(region, tagKey, tagValue),
				Integer.parseInt(env("AWS_REAL_WARM", "20"))));
			break;

		case "denied":
			Log.setLevel(Log.Level.INFO);
			Log.setCallback((level, message) -> messages.add(message));
			System.out.println(denied(provider(region, tagKey, tagValue)));
			break;

		case "join":
			Log.setLevel(Log.Level.DEBUG);
			Log.setCallback((level, message) -> {
				if (level == Log.Level.WARN || level == Log.Level.ERROR || message.startsWith("AWS EC2 seed provider ")) {
					messages.add(message);
				}
			});
			System.out.println(join(region, tagKey, tagValue, Integer.parseInt(env("AWS_REAL_EXPECTED_NODES", null)),
				env("AWS_REAL_NAMESPACE", "test"), Integer.parseInt(env("AWS_REAL_REFRESHES", "30"))));
			break;

		default:
			System.err.println("unknown mode " + args[0]);
			System.exit(2);
		}
		System.exit(0);
	}

	private static AwsEc2TagSeedCandidateProvider provider(String region, String tagKey, String tagValue) {
		return AwsEc2TagSeedCandidateProvider.builder().region(region).tagFilter(tagKey, tagValue).build();
	}

	private static String latency(AwsEc2TagSeedCandidateProvider provider, int warm) {
		Json json = new Json().field("mode", "latency");
		Host[] none = new Host[0];
		long begin = System.nanoTime();
		List<Host> hosts;

		try {
			hosts = provider.refreshSeedCandidates(new SeedCandidateContext(null, none, 0));
		}
		catch (AerospikeException ae) {
			return json.field("credentials_source", credentialSource()).field("error", message(ae)).end();
		}

		long cold = micros(begin);
		List<Long> warmTimes = new ArrayList<>();
		List<String> warmFailures = new ArrayList<>();

		for (int i = 1; i <= warm; i++) {
			begin = System.nanoTime();

			try {
				provider.refreshSeedCandidates(new SeedCandidateContext(null, none, i));
			}
			catch (AerospikeException ae) {
				warmFailures.add(message(ae));
			}
			warmTimes.add(micros(begin));
		}
		return json.field("credentials_source", credentialSource())
			.strings("hosts", hostStrings(hosts))
			.field("cold_us", cold)
			.longs("warm_us", warmTimes)
			.strings("warm_failures", warmFailures)
			.end();
	}

	private static String denied(AwsEc2TagSeedCandidateProvider provider) {
		Json json = new Json().field("mode", "denied");

		try {
			List<Host> hosts = provider.refreshSeedCandidates(new SeedCandidateContext(null, new Host[0], 0));
			return json.field("credentials_source", credentialSource()).strings("hosts", hostStrings(hosts))
				.end();
		}
		catch (AerospikeException ae) {
			return json.field("credentials_source", credentialSource()).field("error", message(ae)).end();
		}
	}

	private static String join(String region, String tagKey, String tagValue, int expectedNodes, String namespace,
		int refreshes) throws InterruptedException {
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryRefreshInterval = policy.tendInterval;
		policy.seedCandidateProvider = provider(region, tagKey, tagValue);
		Json json = new Json().field("mode", "join");

		try (AerospikeClient client = new AerospikeClient(policy)) {
			long deadline = System.currentTimeMillis() + 60000;

			while (client.getNodes().length < expectedNodes || ownedPartitions(client, namespace) < Node.PARTITIONS) {
				if (System.currentTimeMillis() > deadline) {
					return counts(json, client, namespace).field("error", "nodes=" + client.getNodes().length +
						" owned=" + ownedPartitions(client, namespace) + " after 60s").end();
				}
				Thread.sleep(100);
			}

			messages.clear();
			long wait = (long)refreshes * policy.discoveryRefreshInterval + 60000;
			deadline = System.currentTimeMillis() + wait;

			while (refreshes() < refreshes && count("Seed candidate provider exceeded") == 0) {
				if (System.currentTimeMillis() > deadline) {
					return counts(json, client, namespace).field("error", "only " + refreshes() +
						" refreshes in " + wait + "ms").end();
				}
				Thread.sleep(100);
			}
			return counts(json, client, namespace).end();
		}
		catch (AerospikeException ae) {
			return json.field("error", message(ae)).end();
		}
	}

	private static Json counts(Json json, AerospikeClient client, String namespace) {
		List<String> failures = new ArrayList<>();

		for (String m : messages) {
			if (m.contains("Seed candidate refresh failed")) {
				failures.add(m);
			}
		}
		return json.field("nodes", client.getNodes().length)
			.field("owned_partitions", ownedPartitions(client, namespace))
			.field("partitions", Node.PARTITIONS)
			.field("refreshes", refreshes())
			.field("overruns", count("exceeding tend deadline"))
			.strings("failures", failures)
			.field("tripped", count("Seed candidate provider exceeded") > 0);
	}

	private static String credentialSource() {
		String prefix = "AWS EC2 seed provider credentials from ";

		for (String m : messages) {
			if (m.startsWith(prefix)) {
				return m.substring(prefix.length()).trim();
			}
		}
		return null;
	}

	private static int refreshes() {
		int n = 0;

		for (String m : messages) {
			if (m.startsWith("AWS EC2 seed provider ") && m.contains(" returned ")) {
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

	private static int ownedPartitions(AerospikeClient client, String namespace) {
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

	private static List<String> hostStrings(List<Host> hosts) {
		List<String> list = new ArrayList<>();

		for (Host host : hosts) {
			list.add(host.name + ':' + host.port);
		}
		return list;
	}

	private static long micros(long begin) {
		return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - begin);
	}

	private static String message(AerospikeException ae) {
		return ae.getMessage().replace('\n', ' ');
	}

	private static String env(String name, String defaultValue) {
		String value = System.getenv(name);

		if (value == null || value.isEmpty()) {
			if (defaultValue == null) {
				System.err.println(name + " is not set");
				System.exit(2);
			}
			return defaultValue;
		}
		return value;
	}

	private static final class Json {
		private final StringBuilder sb = new StringBuilder("{");

		Json field(String name, Object value) {
			key(name);

			if (value == null) {
				sb.append("null");
			}
			else if (value instanceof Number || value instanceof Boolean) {
				sb.append(value);
			}
			else {
				quote(value.toString());
			}
			return this;
		}

		Json strings(String name, List<String> values) {
			key(name);
			sb.append('[');

			for (int i = 0; i < values.size(); i++) {
				if (i > 0) {
					sb.append(',');
				}
				quote(values.get(i));
			}
			sb.append(']');
			return this;
		}

		Json longs(String name, List<Long> values) {
			key(name);
			sb.append('[');

			for (int i = 0; i < values.size(); i++) {
				if (i > 0) {
					sb.append(',');
				}
				sb.append(values.get(i));
			}
			sb.append(']');
			return this;
		}

		String end() {
			return sb.append('}').toString();
		}

		private void key(String name) {
			if (sb.length() > 1) {
				sb.append(',');
			}
			quote(name);
			sb.append(':');
		}

		private void quote(String s) {
			sb.append('"');

			for (int i = 0; i < s.length(); i++) {
				char c = s.charAt(i);

				switch (c) {
				case '"': sb.append("\\\""); break;
				case '\\': sb.append("\\\\"); break;
				case '\n': sb.append("\\n"); break;
				case '\r': sb.append("\\r"); break;
				case '\t': sb.append("\\t"); break;
				default:
					if (c < 0x20) {
						sb.append(String.format("\\u%04x", (int)c));
					}
					else {
						sb.append(c);
					}
				}
			}
			sb.append('"');
		}
	}
}
