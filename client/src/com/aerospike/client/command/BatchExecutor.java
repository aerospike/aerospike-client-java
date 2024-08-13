/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.command;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.BatchPolicy;

public final class BatchExecutor {

	public static void execute(Cluster cluster, BatchPolicy policy, IBatchCommand[] commands, BatchStatus status) {
		cluster.addCommandCount();

		if (commands.length <= 1) {
			// Run batch request in same thread.
			for (IBatchCommand command : commands) {
				try {
					command.execute();
				}
				catch (AerospikeException ae) {
					if (ae.getInDoubt()) {
						command.setInDoubt();
					}
					status.setException(ae);
				}
				catch (Throwable e) {
					command.setInDoubt();
					status.setException(new AerospikeException(e));
				}
			}
			status.checkException();
			return;
		}

		// Start virtual threads.
		try (ExecutorService es = Executors.newThreadPerTaskExecutor(cluster.threadFactory);) {
			for (IBatchCommand command : commands) {
				es.execute(command);
			}
		}

		// Throw an exception if an error occurred.
		status.checkException();
	}
}
