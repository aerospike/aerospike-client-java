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

import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;

public abstract class AsyncExample {
	/**
	 * Connect and run one or more asynchronous client examples.
	 */
	public static void runExamples(Console console, Parameters params, List<String> examples) throws Exception {
		AsyncClientPolicy policy = new AsyncClientPolicy();
		policy.user = params.user;
		policy.password = params.password;
		policy.asyncMaxCommands = 300;
		policy.asyncSelectorThreads = 1;
		policy.asyncSelectorTimeout = 10;
		policy.failIfNotConnected = true;
		
		params.policy = policy.asyncReadPolicyDefault;
		params.writePolicy = policy.asyncWritePolicyDefault;
		
		AsyncClient client = new AsyncClient(policy, params.host, params.port);

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
	 * Run asynchronous client example.
	 */
	public static void runExample(String exampleName, AsyncClient client, Parameters params, Console console) throws Exception {
		String fullName = "com.aerospike.examples." + exampleName;
		Class<?> cls = Class.forName(fullName);

		if (AsyncExample.class.isAssignableFrom(cls)) {
			Constructor<?> ctor = cls.getDeclaredConstructor(Console.class);
			AsyncExample example = (AsyncExample)ctor.newInstance(console);
			example.run(client, params);
		}
		else {
			console.error("Invalid example: " + exampleName);
		}
	}

	protected Console console;

	public AsyncExample(Console console) {
		this.console = console;
	}

	public void run(AsyncClient client, Parameters params) throws Exception {
		console.info(this.getClass().getSimpleName() + " Begin");
		runExample(client, params);
		console.info(this.getClass().getSimpleName() + " End");
	}

	public abstract void runExample(AsyncClient client, Parameters params) throws Exception;
}
