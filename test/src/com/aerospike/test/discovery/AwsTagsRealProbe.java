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
package com.aerospike.test.discovery;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider;
import com.aerospike.client.discovery.SeedCandidateContext;

/**
 * One fresh-JVM run of {@link AwsEc2TagSeedCandidateProvider} against real AWS, started by
 * {@link TestAwsTagsReal}: a cold first refresh (credentials included), then warm tend refreshes.
 * Prints one {@code PROBE} line per result.
 * <p>
 * Arguments: region, tag key, tag value, warm refresh count.
 */
public final class AwsTagsRealProbe {
	public static void main(String[] args) {
		List<String> messages = new CopyOnWriteArrayList<>();
		Log.setLevel(Log.Level.INFO);
		Log.setCallback((level, message) -> messages.add(level + " " + message));

		AwsEc2TagSeedCandidateProvider provider = AwsEc2TagSeedCandidateProvider.builder()
			.region(args[0])
			.tagFilter(args[1], args[2])
			.build();
		int warm = Integer.parseInt(args[3]);
		Host[] none = new Host[0];

		try {
			long begin = System.nanoTime();
			List<Host> hosts = provider.refreshSeedCandidates(new SeedCandidateContext(null, none, 0));
			long cold = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - begin);
			List<Long> warmTimes = new ArrayList<>();
			int warmFailures = 0;

			for (int i = 1; i <= warm; i++) {
				begin = System.nanoTime();

				try {
					provider.refreshSeedCandidates(new SeedCandidateContext(null, none, i));
				}
				catch (AerospikeException ae) {
					warmFailures++;
					System.out.println("PROBE warmFailure " + ae.getMessage().replace('\n', ' '));
				}
				warmTimes.add(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - begin));
			}

			for (String m : messages) {
				if (m.contains("credentials from")) {
					System.out.println("PROBE credentials " + m);
				}
			}
			System.out.println("PROBE coldMicros " + cold);
			System.out.println("PROBE warmMicros " + warmTimes);
			System.out.println("PROBE warmFailures " + warmFailures);
			System.out.println("PROBE hosts " + hosts);
		}
		catch (AerospikeException ae) {
			System.out.println("PROBE error " + ae.getMessage().replace('\n', ' '));
		}
	}
}
