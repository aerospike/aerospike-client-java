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
package com.aerospike.test.translation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;

/**
 * {@link ClientPolicy#useServicesAlternate} with nodes that have no alternate access address,
 * against two two node clusters started by test/translation/run_alt_missing.sh. In the missing
 * cluster node A advertises 127.0.0.1 and its published host port as its alternate access address
 * and node B has none. In the control cluster nodes C and D both advertise one. The clusters arrive
 * as system properties:
 * <ul>
 * <li>alt.host: host every node is published on</li>
 * <li>alt.nodeA, alt.portA: missing cluster node with an alternate address</li>
 * <li>alt.nodeB, alt.portB: missing cluster node without an alternate address</li>
 * <li>alt.nodeC, alt.portC, alt.nodeD: control cluster nodes</li>
 * </ul>
 */
public class TestAlternateMissing {
	private static final int TEND_INTERVAL = 250;
	private static final int MISSING_TENDS = 8;
	private static final int JOIN_WAIT_MS = 10000;
	private static final List<String> errors = new CopyOnWriteArrayList<>();

	private static String host;
	private static String nodeA;
	private static int portA;
	private static String nodeB;
	private static int portB;
	private static String nodeC;
	private static int portC;
	private static String nodeD;

	@BeforeClass
	public static void init() {
		host = required("alt.host");
		nodeA = required("alt.nodeA");
		portA = Integer.parseInt(required("alt.portA"));
		nodeB = required("alt.nodeB");
		portB = Integer.parseInt(required("alt.portB"));
		nodeC = required("alt.nodeC");
		portC = Integer.parseInt(required("alt.portC"));
		nodeD = required("alt.nodeD");

		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.ERROR) {
				errors.add(message);
			}
			System.out.println("CLIENT-LOG " + level + ' ' + message);
		});
	}

	@Before
	public void clearErrors() {
		errors.clear();
	}

	@Test
	public void peerWithoutAlternateIsNotAdded() throws Exception {
		String peersAlt = Info.request(host, portA, "peers-clear-alt");
		System.out.println("ALT-MISSING-PEERS " + nodeA + " peers-clear-alt=" + peersAlt);

		try (AerospikeClient client = new AerospikeClient(policy(), new Host(host, portA))) {
			for (int i = 0; i < MISSING_TENDS; i++) {
				Thread.sleep(TEND_INTERVAL);
				assertEquals(List.of(nodeA), nodeNames(client));
			}

			List<String> missing = missingErrors(nodeB);
			System.out.println("ALT-MISSING-RESULT peer-missing nodes=" + client.getNodes().length + ' ' +
				nodeNames(client) + " errors=" + missing.size());
			assertTrue("No error logged for node " + nodeB + ": " + errors, missing.size() > 0);
			System.out.println("ALT-MISSING-LOG peer-missing " + missing.get(0));
			assertTrue("Node " + nodeB + " not retried on a later tend: " + missing, missing.size() >= 2);
		}
	}

	@Test
	public void seedWithoutAlternateFailsInit() {
		AerospikeException.AlternateAddressMissing e = assertThrows(AerospikeException.AlternateAddressMissing.class,
			() -> new AerospikeClient(policy(), new Host(host, portB)).close());

		System.out.println("ALT-MISSING-RESULT seed-missing exception=" + e.getClass().getName());
		System.out.println("ALT-MISSING-LOG seed-missing " + e.getMessage());
		assertTrue(e.getMessage(), e.getMessage().contains(nodeB));
		assertTrue(e.getMessage(), e.getMessage().contains("useServicesAlternate"));
	}

	@Test
	public void bothAlternateJoin() throws Exception {
		try (AerospikeClient client = new AerospikeClient(policy(), new Host(host, portC))) {
			long deadline = System.currentTimeMillis() + JOIN_WAIT_MS;

			while (client.getNodes().length < 2 && System.currentTimeMillis() < deadline) {
				Thread.sleep(TEND_INTERVAL);
			}

			List<String> names = nodeNames(client);
			System.out.println("ALT-MISSING-RESULT control nodes=" + names.size() + ' ' + names + " errors=" + errors.size());
			assertEquals(List.of(nodeC, nodeD), names);
			assertEquals(List.of(), errors);
		}
	}

	private static ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.useServicesAlternate = true;
		policy.tendInterval = TEND_INTERVAL;
		return policy;
	}

	private static List<String> nodeNames(AerospikeClient client) {
		List<String> names = new ArrayList<>();

		for (Node node : client.getNodes()) {
			names.add(node.getName());
		}
		names.sort(null);
		return names;
	}

	private static List<String> missingErrors(String nodeName) {
		List<String> list = new ArrayList<>();

		for (String error : errors) {
			if (error.contains("Node " + nodeName) && error.contains("useServicesAlternate")) {
				list.add(error);
			}
		}
		return list;
	}

	private static String required(String name) {
		String value = System.getProperty(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("System property " + name + " is not set. Run test/translation/run_alt_missing.sh");
		}
		return value;
	}
}
