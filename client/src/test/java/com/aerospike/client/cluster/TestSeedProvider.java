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
package com.aerospike.client.cluster;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import com.aerospike.client.Host;
import com.aerospike.client.discovery.SeedCandidateProvider;

final class TestSeedProvider implements SeedCandidateProvider {
	final AtomicInteger calls = new AtomicInteger();
	private final boolean periodic;
	private final boolean deadline;
	volatile IntFunction<List<Host>> result;
	volatile long sleepMillis;
	volatile Throwable error;

	TestSeedProvider(boolean periodic, boolean deadline, Host... hosts) {
		this.periodic = periodic;
		this.deadline = deadline;
		List<Host> list = List.of(hosts);
		this.result = call -> list;
	}

	@Override
	public List<Host> refreshSeedCandidates() {
		int call = calls.incrementAndGet();

		if (sleepMillis > 0) {
			try {
				Thread.sleep(sleepMillis);
			}
			catch (InterruptedException ie) {
				throw new RuntimeException(ie);
			}
		}

		Throwable t = error;

		if (t != null) {
			throw TestSeedProvider.<RuntimeException>sneaky(t);
		}
		return result.apply(call);
	}

	@Override
	public boolean needsPeriodicRefresh() {
		return periodic;
	}

	@Override
	public boolean supportsDeadline() {
		return deadline;
	}

	@SuppressWarnings("unchecked")
	private static <T extends Throwable> T sneaky(Throwable t) throws T {
		throw (T)t;
	}
}
