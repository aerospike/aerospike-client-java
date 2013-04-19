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
