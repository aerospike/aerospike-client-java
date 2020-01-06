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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.dto.KeysExists;
import com.aerospike.client.reactor.dto.KeysRecords;
import com.aerospike.client.reactor.util.Args;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class BatchReactorFailTest extends ReactorFailTest {
	private static final String keyPrefix = "batchkey";
	private static final String valuePrefix = "batchvalue";
	private final String binName = args.getBinName("batchbin");
	private static final int size = 8;
	private Key[] sendKeys;

	public BatchReactorFailTest(Args args) {
		super(args);
	}

	@Before
	public void fillData() {
		sendKeys = new Key[size];

		WritePolicy policy = new WritePolicy();
		policy.expiration = 2592000;

		Mono.zip(
				IntStream.range(0, size)
						.mapToObj(i -> {
							final Key key = new Key(args.namespace, args.set, keyPrefix + (i + 1));
							sendKeys[i] = key;
							Bin bin = new Bin(binName, valuePrefix + (i + 1));
							return reactorClient.put(policy, key, bin);
						}).collect(Collectors.toList()),
				objects -> objects).block();
	}

	@Test
	public void shouldFailOnBatchExistsArray() {
		Mono<KeysExists> mono = proxyReactorClient.exists(strictBatchPolicy(), sendKeys);

		StepVerifier.create(mono)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}

	@Test
	public void shouldFailOnBatchExistsSequence() {

		Flux<KeyRecord> flux = proxyReactorClient.getFlux(strictBatchPolicy(), sendKeys);

		StepVerifier.create(flux)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}

	@Test
	public void shouldFailOnBatchGetArray() {
		Mono<KeysRecords> mono = proxyReactorClient.get(strictBatchPolicy(), sendKeys);

		StepVerifier.create(mono)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}

	@Test
	public void shouldFailOnBatchGetSequence() {
		Flux<KeyRecord> flux = proxyReactorClient.getFlux(strictBatchPolicy(), sendKeys);

		StepVerifier.create(flux)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}

	@Test
	public void shouldFailOnBatchGetHeaders() {
		Mono<KeysRecords> mono = proxyReactorClient.getHeaders(strictBatchPolicy(), sendKeys);

		StepVerifier.create(mono)
				.expectError(AerospikeException.Timeout.class)
				.verify();
	}

	@Test
	public void shouldFailOnBatchReadComplex() {
		// Batch gets into one call.
		// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.
		String[] bins = new String[] {binName};
		List<BatchRead> records = new ArrayList<BatchRead>();
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 1), bins));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 2), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 3), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 4), false));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 5), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 6), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 7), bins));

		// This record should be found, but the requested bin will not be found.
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 8), new String[] {"binnotfound"}));

		// This record should not be found.
		records.add(new BatchRead(new Key(args.namespace, args.set, "keynotfound"), bins));

		// Execute batch.
		Mono<List<BatchRead>> mono = proxyReactorClient.get(strictBatchPolicy(), records);

		StepVerifier.create(mono)
				.expectError(AerospikeException.Timeout.class)
				.verify();

	}
}
