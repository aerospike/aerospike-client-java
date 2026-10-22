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
import com.aerospike.test.discovery.TestL2ReachableNode;
import com.aerospike.test.discovery.TestL2Topology;
import com.aerospike.test.util.Args;

/**
 * Runs the discovery tests. {@link TestL2Topology} runs against the L2 topology built by
 * .github/workflows/discovery-l2.yaml or test/discovery/run_l2.sh. {@link TestL2ReachableNode}
 * needs the reachable profile of run_l2.sh and is skipped without it.
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
	TestL2ReachableNode.class
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
