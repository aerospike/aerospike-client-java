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
package com.aerospike.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchExecutor;
import com.aerospike.client.command.BatchStatus;
import com.aerospike.client.command.IBatchCommand;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;

/**
 * BatchExecutor waits for its sub-commands by leaving a try-with-resources block, which calls
 * the default ExecutorService.close(). That default waits in awaitTermination(1, DAYS) inside a
 * "while (!terminated)" loop, so it re-arms indefinitely and has no timeout. The batch policy's
 * own totalTimeout is never consulted.
 * <p>
 * A sub-command can outlive totalTimeout: SyncCommand only compares against its deadline between
 * retries, and a single attempt is bounded per socket read by socketTimeout rather than in total,
 * so a slow-feeding server keeps one attempt alive. When that happens the calling thread stays in
 * close() for as long as the sub-command runs.
 * <p>
 * These tests need no server. They drive BatchExecutor directly with sub-commands that stall, so
 * the wait is the only thing under test.
 */
public class BatchExecutorTimeoutTest {
	/** Longest a sub-command will stall for. Comfortably longer than any totalTimeout used here. */
	private static final long STALL_MILLIS = 10_000;

	/**
	 * A sub-command that stalls until interrupted, standing in for an attempt that outlives
	 * totalTimeout. It reports when it started so the test never measures thread startup.
	 */
	private static final class StallingCommand implements IBatchCommand {
		private final CountDownLatch started;

		private StallingCommand(CountDownLatch started) {
			this.started = started;
		}

		@Override
		public void execute() {
			started.countDown();

			try {
				Thread.sleep(STALL_MILLIS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		@Override
		public void setInDoubt() {
		}

		@Override
		public void run() {
			execute();
		}
	}

	/**
	 * Builds a cluster without contacting a server. BatchExecutor only needs it for its thread
	 * factory and command counter, so a cluster that never connects is enough.
	 */
	private static AerospikeClient offlineClient() {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 500;

		// Port 1 is privileged and unused, so tending cannot succeed.
		return new AerospikeClient(policy, "127.0.0.1", 1);
	}

	@Test
	public void batchStopsWaitingAtTotalTimeout() throws Exception {
		final int totalTimeout = 500;

		try (AerospikeClient client = offlineClient()) {
			Cluster cluster = client.cluster;

			BatchPolicy policy = new BatchPolicy();
			policy.totalTimeout = totalTimeout;

			CountDownLatch started = new CountDownLatch(2);
			IBatchCommand[] commands = {new StallingCommand(started), new StallingCommand(started)};
			BatchStatus status = new BatchStatus(false);

			// More than one command, otherwise BatchExecutor runs on the caller's thread.
			assertTrue("commands must exceed 1 to reach the virtual thread path", commands.length > 1);

			long begin = System.nanoTime();

			try {
				BatchExecutor.execute(cluster, policy, commands, status);
				fail("Expected a timeout because both sub-commands outlive totalTimeout");
			}
			catch (AerospikeException.Timeout expected) {
				// The caller must be told the batch was abandoned, otherwise it reads a
				// partially populated result array as if the batch had succeeded.
			}

			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

			assertTrue("sub-commands should have started", started.await(1, TimeUnit.SECONDS));
			assertTrue("BatchExecutor returned after " + elapsed + "ms with totalTimeout="
				+ totalTimeout + "ms; it waited for the stalled sub-commands instead",
				elapsed < STALL_MILLIS / 2);
		}
	}

	/**
	 * totalTimeout of zero means no client-side limit, which is the documented behaviour and the
	 * default for scan and query. The wait must stay unbounded there.
	 */
	@Test
	public void batchWaitsWhenTotalTimeoutIsZero() throws Exception {
		try (AerospikeClient client = offlineClient()) {
			Cluster cluster = client.cluster;

			BatchPolicy policy = new BatchPolicy();
			policy.totalTimeout = 0;

			CountDownLatch started = new CountDownLatch(2);
			IBatchCommand[] commands = {new StallingCommand(started), new StallingCommand(started)};
			BatchStatus status = new BatchStatus(false);

			Thread caller = new Thread(() -> BatchExecutor.execute(cluster, policy, commands, status));
			caller.setDaemon(true);
			caller.start();

			assertTrue("sub-commands should have started", started.await(5, TimeUnit.SECONDS));

			caller.join(1000);
			assertTrue("BatchExecutor must keep waiting when totalTimeout is zero", caller.isAlive());

			caller.interrupt();
		}
	}
}
