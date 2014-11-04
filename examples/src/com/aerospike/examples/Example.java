/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
package com.aerospike.examples;

import java.lang.reflect.Constructor;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;

public abstract class Example {

	protected static final int DEFAULT_TIMEOUT_MS = 1000;

	/**
	 * Connect and run one or more client examples.
	 */
	public static void runExamples(Console console, Parameters params, List<String> examples) throws Exception {
		ClientPolicy policy = new ClientPolicy();
		policy.user = params.user;
		policy.password = params.password;
		policy.failIfNotConnected = true;
		
		params.policy = policy.readPolicyDefault;
		params.writePolicy = policy.writePolicyDefault;

		AerospikeClient client = new AerospikeClient(policy, params.host, params.port);

		try {
			params.setServerSpecific(client);

			for (String exampleName : examples) {
				runExample(exampleName, client, params, console);
			}
		}
		finally {
			client.close();
		}
	}

	/**
	 * Run client example.
	 */
	public static void runExample(String exampleName, AerospikeClient client, Parameters params, Console console) throws Exception {
		String fullName = "com.aerospike.examples." + exampleName;
		Class<?> cls = Class.forName(fullName);

		if (Example.class.isAssignableFrom(cls)) {
			Constructor<?> ctor = cls.getDeclaredConstructor(Console.class);
			Example example = (Example)ctor.newInstance(console);
			example.run(client, params);
		}
		else {
			console.error("Invalid example: " + exampleName);
		}
	}

	protected Console console;

	public Example(Console console) {
		this.console = console;
	}

	public void run(AerospikeClient client, Parameters params) throws Exception {
		console.info(this.getClass().getSimpleName() + " Begin");
		runExample(client, params);
		console.info(this.getClass().getSimpleName() + " End");
	}

	public abstract void runExample(AerospikeClient client, Parameters params) throws Exception;
}
