/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.task;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.util.Util;

/**
 * Task used to poll for server task completion.
 */
public abstract class Task {
	protected final Cluster cluster;
	private boolean done;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public Task(Cluster cluster, boolean done) {
		this.cluster = cluster;
		this.done = done;
	}

	/**
	 * Wait for asynchronous task to complete using default sleep interval.
	 */
	public final void waitTillComplete() throws AerospikeException {
		waitTillComplete(1000);
	}

	/**
	 * Wait for asynchronous task to complete using given sleep interval.
	 */
	public final void waitTillComplete(int sleepInterval) throws AerospikeException {
		while (! done) {
			Util.sleep(sleepInterval);
			done = isDone();
		}
	}

	/**
	 * Query all nodes for task completion status.
	 */
	public abstract boolean isDone() throws AerospikeException;
}
