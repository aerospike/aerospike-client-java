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
		policy.asyncMaxCommands = 300;
		policy.asyncSelectorThreads = 1;
		policy.asyncSelectorTimeout = 10;
		
		AsyncClient client = new AsyncClient(policy, params.host, params.port);

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
