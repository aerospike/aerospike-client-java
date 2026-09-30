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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.discovery.TestDiscoveryConfig;
import com.aerospike.test.util.Args;

/**
 * Runs the discovery tests against a live server.
 *
 * <p>The suite exposes the {@link ClientPolicy} it built so tests can assert that a
 * discovery flag passed in {@code -Dargs} actually reached the client.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestDiscoveryConfig.class
})
public class SuiteDiscovery {
	public static IAerospikeClient client = null;
	public static ClientPolicy clientPolicy = null;

	@BeforeClass
	public static void init() {
		Log.setCallback(null);

		System.out.println("Begin AerospikeClient");
		Args args = Args.Instance;

		ClientPolicy policy = new ClientPolicy();
		args.setClientPolicy(policy);
		clientPolicy = policy;

		Host[] hosts = Host.parseHosts(args.host, args.port);

		client = new AerospikeClient(policy, hosts);

		try {
			args.setServerSpecific(client);
		}
		catch (RuntimeException re) {
			client.close();
			throw re;
		}
	}

	@AfterClass
	public static void destroy() {
		System.out.println("End AerospikeClient");
		if (client != null) {
			client.close();
		}
	}
}
