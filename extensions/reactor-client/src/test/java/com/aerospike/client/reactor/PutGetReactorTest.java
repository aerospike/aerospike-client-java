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

import static com.aerospike.client.reactor.util.ReactorUtil.succeedAfterRetries;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.util.Args;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class PutGetReactorTest extends ReactorTest {

	private final String binName = args.getBinName("putgetbin");

	public PutGetReactorTest(Args args) {
		super(args);
	}

	@Test
	public void putGetInline() {
		final Key key = new Key(args.namespace, args.set, "putgetkey1");
		String binValue = "value";
		final Bin bin = new Bin(binName, binValue);

		Mono<KeyRecord> mono = reactorClient.put(key, bin)
				.flatMap(key1 -> reactorClient.get(key))
				.doOnNext(keyRecord -> assertBinEqual(keyRecord.key, keyRecord.record, bin));

		StepVerifier.create(mono)
				.expectNextMatches(checkKeyRecord(key, binName, binValue))
				.verifyComplete();
	}

	@Test
	public void putGetHeader() {
		final Key key = new Key(args.namespace, args.set, "putgetheaderkey1");
		String binValue = "value";
		final Bin bin = new Bin(binName, binValue);

		Mono<KeyRecord> mono = reactorClient.put(key, bin)
				.flatMap(key1 -> reactorClient.getHeader(key));

		StepVerifier.create(mono)
				.expectNextMatches(keyRecord -> key.equals(keyRecord.key) && keyRecord.record != null)
				.verifyComplete();
	}

	@Test
	public void putGetWithRetry() {
		final Key key = new Key(args.namespace, args.set, "putgetkey2");
		String binValue = "value";
		final Bin bin = new Bin(binName, binValue);

		AtomicInteger putFailsCount = new AtomicInteger(2);
		AtomicInteger getFailsCount = new AtomicInteger(2);
		Mono<KeyRecord> mono = succeedAfterRetries(reactorClient.put(key, bin), putFailsCount, new ConnectException())
				.flatMap(key1 -> succeedAfterRetries(reactorClient.get(key), getFailsCount, new ConnectException()))
				.doOnNext(keyRecord -> assertBinEqual(keyRecord.key, keyRecord.record, bin))
				.retry(putFailsCount.get() * getFailsCount.get() + 1, t -> t instanceof ConnectException);

		StepVerifier.create(mono)
				.expectNextMatches(checkKeyRecord(key, binName, binValue))
				.verifyComplete();
	}


}
