package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;

public abstract class Example {

	protected static final int DEFAULT_TIMEOUT_MS = 1000;

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
