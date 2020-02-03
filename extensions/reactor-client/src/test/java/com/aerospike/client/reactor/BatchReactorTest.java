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

import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.dto.KeyExists;
import com.aerospike.client.reactor.dto.KeysExists;
import com.aerospike.client.reactor.dto.KeysRecords;
import com.aerospike.client.reactor.util.Args;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class BatchReactorTest extends ReactorTest {
	private static final String keyPrefix = "batchkey";
	private static final String valuePrefix = "batchvalue";
	private final String binName = args.getBinName("batchbin");
	private static final int size = 8;
	private Key[] sendKeys;
	private Key[] notSendKeys;

	public BatchReactorTest(Args args) {
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

		notSendKeys = new Key[]{
				new Key(args.namespace, args.set, keyPrefix+"absent1"),
				new Key(args.namespace, args.set, keyPrefix+"absent2")
		};
	}

	@Test
	public void batchExistsArray() {
		Mono<KeysExists> mono = reactorClient.exists(sendKeys);

		StepVerifier.create(mono)
				.expectNextMatches(keysExists -> {
					assertThat(keysExists.exists).hasSameSizeAs(sendKeys).containsOnly(true);
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void batchNotExistsArray() {

		Mono<KeysExists> mono = reactorClient.exists(notSendKeys);

		StepVerifier.create(mono)
				.expectNextMatches(keysExists -> {
					assertThat(keysExists.exists).hasSameSizeAs(notSendKeys).containsOnly(false);
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void batchExistsSequence() {
		Flux<KeyExists> flux = reactorClient.existsFlux(sendKeys);

		StepVerifier.create(flux)
				.recordWith(ArrayList::new)
				.expectNextCount(sendKeys.length)
				.consumeRecordedWith(results -> assertThat(results)
						.extracting(keyExists -> keyExists.exists)
						.containsOnly(true))
				.verifyComplete();
	}

	@Test
	public void batchNotExistsSequence() {
		Flux<KeyExists> flux = reactorClient.existsFlux(notSendKeys);

		StepVerifier.create(flux)
				.recordWith(ArrayList::new)
				.expectNextCount(notSendKeys.length)
				.consumeRecordedWith(results -> assertThat(results)
						.extracting(keyExists -> keyExists.exists)
						.containsOnly(false))
				.verifyComplete();
	}

	@Test
	public void batchGetArray() {
		Mono<KeysRecords> mono = reactorClient.get(sendKeys);

		StepVerifier.create(mono)
				.expectNextMatches(keysRecords -> {
					for (int i = 0; i < keysRecords.records.length; i++) {
						assertBinEqual(keysRecords.keys[i], keysRecords.records[i], binName, valuePrefix + (i + 1));
					}
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void batchGetSequence() {
		Flux<KeyRecord> flux = reactorClient.getFlux(sendKeys);

		StepVerifier.create(flux)
				.recordWith(ArrayList::new)
				.expectNextCount(sendKeys.length)
				.consumeRecordedWith(results -> assertThat(results)
						.extracting(keyRecord -> {
							assertRecordFound(keyRecord.key, keyRecord.record);
							assertThat(keyRecord.record.getValue(binName)).isNotNull();
							return true;
						})
						.containsOnly(true))
				.verifyComplete();
	}

	@Test
	public void batchGetNothingSequence() {
		Flux<KeyRecord> flux = reactorClient.getFlux(notSendKeys);

		StepVerifier.create(flux)
				.recordWith(ArrayList::new)
				.expectNextCount(notSendKeys.length)
				.consumeRecordedWith(results -> assertThat(results)
						.extracting(keyRecord -> keyRecord.record)
						.containsOnlyNulls())
				.verifyComplete();
	}

	@Test
	public void batchGetHeaders() {
		Mono<KeysRecords> mono = reactorClient.getHeaders(sendKeys);

		StepVerifier.create(mono)
				.expectNextMatches(keysRecords -> {
					assertThat(keysRecords.records).hasSize(size);
					for (int i = 0; i < keysRecords.records.length; i++) {
						Record record = keysRecords.records[i];
						assertRecordFound(keysRecords.keys[i], record);
						assertThat(record.generation).isGreaterThan(0);
						assertThat(record.expiration).isGreaterThan(0);
					}
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void batchReadComplex() {
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
		Mono<List<BatchRead>> mono = reactorClient.get(records);

		StepVerifier.create(mono)
				.expectNextMatches(batchReads -> {
					List<BatchRead> recordsFound = batchReads.stream()
							.filter(record -> record.record != null)
							.collect(Collectors.toList());
					assertThat(recordsFound).hasSize(8);
					assertThat(recordsFound).extracting(record -> record.record.getValue(binName))
							.containsExactly(
									"batchvalue1",
									"batchvalue2",
									"batchvalue3",
									//readAllBeans == false
									null,
									"batchvalue5",
									"batchvalue6",
									"batchvalue7",
									//no bean with name "binnotfound"
									null);

					return true;
				})
				.verifyComplete();

	}
}
