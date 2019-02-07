/*
 * Copyright 2012-2018 Aerospike, Inc.
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
package com.aerospike.client.reactor;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.util.Args;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class ScanReactorFailTest extends ReactorFailTest {
	private final String binName = args.getBinName("scanbin");

	public ScanReactorFailTest(Args args) {
		super(args);
	}

	@Test
	public void shouldFailScan() {
		Flux<KeyRecord> flux = proxyReactorClient.scanAll(strictScanPolicy(), args.namespace, args.set, binName);

		StepVerifier.create(flux)
				.expectError(AerospikeException.Timeout.class)
				.verify();
   }
}
