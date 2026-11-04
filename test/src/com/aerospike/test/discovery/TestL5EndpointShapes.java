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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.discovery.L5Client.PodEndpoint;
import com.aerospike.test.util.TestBase;

/**
 * Both address shapes through the PrivateLink endpoint name, each with a fresh client seeded only
 * at the endpoint name and the seed NodePort: pass-through of the alternate list every pod
 * advertises, and a port-map from each node name to the endpoint name and that pod's NodePort.
 */
public class TestL5EndpointShapes extends TestBase {
	private static final String SET = "l5-shapes";

	private L5Environment env;
	private K8sCluster cluster;
	private int foundSize;

	@Before
	public void requireEnvironment() {
		env = L5Environment.require();
		env.waitReady();
		env.waitHealthy();
		cluster = K8sCluster.find();
		foundSize = cluster.size();
		assertEquals("AerospikeCluster size", L5Environment.CLUSTER_SIZE, foundSize);
	}

	@After
	public void restore() {
		Log.setCallback(null);

		if (cluster != null) {
			env.restoreSize(cluster, foundSize);
		}
	}

	/**
	 * Pass-through of the alternate list: the client holds every pod at the endpoint name and the
	 * pod's own NodePort, and serves every partition.
	 */
	@Test
	public void passThroughReachesEveryPodAtItsNodePort() throws Exception {
		ClientPolicy policy = env.policy();
		policy.useServicesAlternate = true;
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		assertShape("pass-through", policy, L5Client.podEndpoints(cluster));
	}

	/**
	 * Port-map from each node name, read from the cluster, to the endpoint name and that pod's
	 * NodePort, on the standard list and with a miss failing the peer: the client holds every pod
	 * at the endpoint name and its NodePort, and serves every partition.
	 */
	@Test
	public void portMapByNodeNameReachesEveryPodAtItsNodePort() throws Exception {
		Map<String,PodEndpoint> endpoints = L5Client.podEndpoints(cluster);
		Map<String,String> portMap = new LinkedHashMap<>();

		for (PodEndpoint endpoint : endpoints.values()) {
			portMap.put(endpoint.nodeName, env.advertised(endpoint.nodePort));
		}
		System.out.println("L5-PORT-MAP " + portMap);

		ClientPolicy policy = env.policy();
		policy.useServicesAlternate = false;
		policy.addressTranslator = new PortMapAddressTranslator(portMap);
		policy.translationOnMiss = TranslationMissMode.STRICT;
		assertShape("port-map", policy, endpoints);
	}

	/**
	 * Every pod advertises the endpoint name verbatim with its own NodePort, as its own alternate
	 * address and in its peers-clear-alt reply for every other pod.
	 */
	@Test
	public void everyPodAdvertisesEndpointNameWithItsOwnNodePort() throws Exception {
		Map<String,PodEndpoint> endpoints = L5Client.podEndpoints(cluster);
		ClientPolicy policy = env.policy();
		policy.useServicesAlternate = true;
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		L5Client.captureLog();

		try (AerospikeClient client = new AerospikeClient(policy, env.seed())) {
			L5Client.awaitRouting("advertised", client, endpoints.size(), L5Client.ROUTING_WAIT_MS);
			L5Client.assertAdvertisement("advertised", env, client, endpoints);
		}
	}

	/**
	 * The endpoint name resolves on the runner, and in the producer VPC only on the control run
	 * that makes it resolvable there.
	 */
	@Test
	public void endpointNameResolvesOnlyWhereExpected() {
		String expect = "resolvable".equals(env.control) ? "yes" : "no";
		L5Environment.Result result = env.ctl(L5Environment.CTL_TIMEOUT_MS, "resolve-check", "--expect-producer", expect);
		System.out.println("L5-RESOLVE-CHECK control=" + env.control + " expectProducer=" + expect + " exit=" + result.exit +
			" lines=" + result.tagged("L5-RESOLVE"));
		result.assertSucceeded();
	}

	private void assertShape(String shape, ClientPolicy policy, Map<String,PodEndpoint> endpoints) throws Exception {
		L5Client.captureLog();

		try (AerospikeClient client = new AerospikeClient(policy, env.seed())) {
			System.out.println("L5-SHAPE shape=" + shape + " seed=" + env.endpointName + ':' + env.seedPort + " useServicesAlternate=" +
				policy.useServicesAlternate + " translator=" + policy.addressTranslator.getClass().getSimpleName() +
				" translationOnMiss=" + policy.translationOnMiss + " pods=" + List.copyOf(endpoints.keySet()));
			L5Client.awaitRouting(shape, client, L5Environment.CLUSTER_SIZE, L5Client.ROUTING_WAIT_MS);
			L5Client.assertNodesAtEndpoint(shape, env, client, endpoints);
			L5Client.assertEveryPartitionServed(shape, client, SET);
		}
	}
}
