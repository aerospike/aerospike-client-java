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

import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.async.DefaultAsyncClient;

public abstract class AsyncExample {
	/**
	 * Connect and run one or more asynchronous client examples.
	 */
	public static void runExamples(Console console, Parameters params, List<String> examples) throws Exception {
		AsyncClientPolicy policy = new AsyncClientPolicy();
		policy.asyncMaxCommands = 300;
		policy.asyncSelectorThreads = 1;
		policy.asyncSelectorTimeout = 10;
		
		AsyncClient client = new DefaultAsyncClient(policy, params.host, params.port);

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
