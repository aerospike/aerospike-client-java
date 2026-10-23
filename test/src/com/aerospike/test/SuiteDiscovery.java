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
package com.aerospike.test;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.aerospike.client.Log;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.discovery.TestDiscoveryConfig;
import com.aerospike.test.discovery.TestL2CnOnlyCertificate;
import com.aerospike.test.discovery.TestL2FaultInjection;
import com.aerospike.test.discovery.TestL2IdleReset;
import com.aerospike.test.discovery.TestL2LowMtu;
import com.aerospike.test.discovery.TestL2PassThrough;
import com.aerospike.test.discovery.TestL2PortMapRange;
import com.aerospike.test.discovery.TestL2ReachableNode;
import com.aerospike.test.discovery.TestL2SourcePortRange;
import com.aerospike.test.discovery.TestL2Topology;
import com.aerospike.test.util.Args;

/**
 * Runs the discovery tests. {@link TestL2Topology} runs against the L2 topology built by
 * .github/workflows/discovery-l2.yaml or test/discovery/run_l2.sh, as does
 * {@link TestL2FaultInjection}. {@link TestL2ReachableNode}, {@link TestL2PassThrough},
 * {@link TestL2PortMapRange}, {@link TestL2CnOnlyCertificate}, {@link TestL2LowMtu},
 * {@link TestL2SourcePortRange} and {@link TestL2IdleReset} need the reachable, pass-through-names or
 * pass-through-ports, port-map-range, cn-only, low-mtu, port-range and idle-reset profiles of
 * run_l2.sh and are skipped without them.
 *
 * <p>The suite builds no shared client: the seed is reachable only through the endpoint and its
 * peers only through a translator, so each test class builds its own {@link ClientPolicy}. The
 * suite exposes the {@link ClientPolicy} built from {@code -Dargs} so tests can assert that a
 * discovery flag actually reached the policy.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestDiscoveryConfig.class,
	TestL2Topology.class,
	TestL2ReachableNode.class,
	TestL2PassThrough.class,
	TestL2PortMapRange.class,
	TestL2CnOnlyCertificate.class,
	TestL2FaultInjection.class,
	TestL2LowMtu.class,
	TestL2SourcePortRange.class,
	TestL2IdleReset.class
})
public class SuiteDiscovery {
	public static ClientPolicy clientPolicy = null;

	@BeforeClass
	public static void init() {
		Log.setCallback(null);

		ClientPolicy policy = new ClientPolicy();
		Args.Instance.setClientPolicy(policy);
		clientPolicy = policy;
	}
}
