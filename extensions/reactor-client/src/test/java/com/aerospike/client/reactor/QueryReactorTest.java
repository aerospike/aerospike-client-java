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
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.client.reactor.util.Args;
import com.aerospike.client.task.IndexTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryReactorTest extends ReactorTest {
	private static final String indexName = "asqindex";
	private static final String keyPrefix = "asqkey";
	private final String binName = args.getBinName("asqbin");
	private static final int size = 50;

	public QueryReactorTest(Args args) {
		super(args);
	}

	@Before
	public void buildIndex() {
		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client.createIndex(policy, args.namespace, args.set, indexName, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	@After
	public void deleteIndex() {
		IndexTask task = client.dropIndex(null, args.namespace, args.set, indexName);
		task.waitTillComplete();
	}

	@Test
	public void query() {

		int begin = 26;
		int end = 34;

		Flux<KeyRecord> flux = Mono.zip(
				IntStream.range(0, size)
						.mapToObj(i -> {
							final Key key = new Key(args.namespace, args.set, keyPrefix + i);
							Bin bin = new Bin(binName, i);
							return reactorClient.put(key, bin);
						}).collect(Collectors.toList()),
				objects -> objects)
		.flatMapMany(objects -> {
			Statement stmt = new Statement();
			stmt.setNamespace(args.namespace);
			stmt.setSetName(args.set);
			stmt.setBinNames(binName);
			stmt.setFilter(Filter.range(binName, begin, end));

			return reactorClient.query(stmt);
		});

		StepVerifier.create(flux)
				.recordWith(ArrayList::new)
				.expectNextCount(9)
				.consumeRecordedWith(results -> {
					assertThat(results)
							.extracting(keyRecord -> {
								int result = keyRecord.record.getInt(binName);
								assertThat(result).isBetween(26, 34);
								return true;
							})
							.isSubsetOf(true);

				})
				.verifyComplete();

	}

}
