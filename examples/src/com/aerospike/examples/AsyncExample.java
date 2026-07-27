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
package com.aerospike.examples;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public abstract class AsyncExample {
	protected Console console;
	private IAerospikeClient client;
	private EventLoop eventLoop;
	private Parameters params;
	private int pendingRuns;
	private boolean runStarted;
	private Throwable failure;

	void initialize(IAerospikeClient client, EventLoop eventLoop, Parameters params, Console console) {
		this.client = client;
		this.eventLoop = eventLoop;
		this.params = params;
		this.console = console;
		this.pendingRuns = 0;
		this.runStarted = false;
		this.failure = null;
	}

	protected final synchronized void beginRun() {
		runStarted = true;
		pendingRuns++;
	}

	protected final void completeRun() {
		finishRun(null);
	}

	protected final void failRun(Throwable t) {
		finishRun(t);
	}

	private synchronized void finishRun(Throwable t) {
		if (t != null && failure == null) {
			failure = t;
		}

		if (pendingRuns > 0) {
			pendingRuns--;
		}
		else if (failure == null) {
			failure = new IllegalStateException("Async example completed a run that was never started");
		}

		if (pendingRuns == 0) {
			notifyAll();
		}
	}

	void awaitCompletion() throws Exception {
		Throwable captured;

		synchronized (this) {
			if (! runStarted) {
				throw new IllegalStateException("Async example returned without starting a root run");
			}

			while (pendingRuns > 0) {
				wait();
			}
			captured = failure;
		}

		if (captured != null) {
			if (captured instanceof Exception) {
				throw (Exception)captured;
			}
			if (captured instanceof Error) {
				throw (Error)captured;
			}
			throw new RuntimeException(captured);
		}
	}

	protected IAerospikeClient client() {
		return client;
	}

	protected EventLoop eventLoop() {
		return eventLoop;
	}

	protected String namespace() {
		return params.namespace;
	}

	protected String set() {
		return params.set;
	}

	protected WritePolicy writePolicy() {
		return params.writePolicy;
	}

	protected Policy readPolicy() {
		return params.policy;
	}

	protected Parameters params() {
		return params;
	}

	public abstract void runExample() throws Exception;
}
