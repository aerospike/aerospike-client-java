/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
		AerospikeClient client = new AerospikeClient(policy, params.host, params.port);

		try {
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
