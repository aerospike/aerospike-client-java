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
	private static Key[] sendKeys;

	public BatchReactorTest(Args args) {
		super(args);
	}

	@Before
	public void fillData() {
		sendKeys = new Key[size];

		WritePolicy policy = new WritePolicy();
		policy.expiration = 2592000;

		Mono.zip(
				IntStream.iterate(0, i -> i + 1)
						.limit(size)
						.mapToObj(i -> {
							final Key key = new Key(args.namespace, args.set, keyPrefix + (i + 1));
							sendKeys[i] = key;
							Bin bin = new Bin(binName, valuePrefix + (i + 1));
							return reactorClient.put(policy, key, bin);
						}).collect(Collectors.toList()),
				objects -> objects).block();
	}

	@Test
	public void batchExistsArray() {
		Mono<KeysExists> mono = reactorClient.exists(sendKeys);

		StepVerifier.create(mono)
				.expectNextMatches(keysExists -> {
					for (int i = 0; i < keysExists.exists.length; i++) {
						assertEquals(true, keysExists.exists[i]);
					}
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
						.isSubsetOf(true))
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
	public void batchGetSequence() throws Exception {
		Flux<KeyRecord> flux = reactorClient.getFlux(sendKeys);

		StepVerifier.create(flux)
				.recordWith(ArrayList::new)
				.expectNextCount(sendKeys.length)
				.consumeRecordedWith(results -> assertThat(results)
						.extracting(keyRecord -> {
							assertRecordFound(keyRecord.key, keyRecord.record);
							assertNotNull(keyRecord.record.getValue(binName));
							return true;
						})
						.isSubsetOf(true))
				.verifyComplete();
   }

	@Test
	public void batchGetHeaders() {
		Mono<KeysRecords> mono = reactorClient.getHeaders(sendKeys);

		StepVerifier.create(mono)
				.expectNextMatches(keysRecords -> {
					assertEquals(size, keysRecords.records.length);
					for (int i = 0; i < keysRecords.records.length; i++) {
						Record record = keysRecords.records[i];
						assertRecordFound(keysRecords.keys[i], record);
						assertGreaterThanZero(record.generation);
						assertGreaterThanZero(record.expiration);
					}
					return true;
				})
				.verifyComplete();
   }

	@Test
	public void batchReadComplex() throws Exception {
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
					int found = 0;
					int count = 0;
					for (BatchRead record : batchReads) {
						Record rec = record.record;
						count++;

						if (rec != null) {
							found++;

							Object value = rec.getValue(binName);

							if (count != 4 && count <= 7) {
								assertEquals(valuePrefix + count, value);
							}
							else {
								assertNull(value);
							}
						}
					}

					assertEquals(8, found);
					return true;
				})
				.verifyComplete();

	}
}
