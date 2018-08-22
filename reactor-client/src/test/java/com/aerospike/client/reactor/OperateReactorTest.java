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

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.util.Args;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OperateReactorTest extends ReactorTest {

	private final String binName = args.getBinName("putgetbin");

	public OperateReactorTest(Args args) {
		super(args);
	}

	@Test
	public void operateList() {
		final Key key = new Key(args.namespace, args.set, "aoplkey1");

		Mono<KeyRecord> mono = reactorClient.delete(key)
				.switchIfEmpty(Mono.just(key))
				.flatMap(key1 -> {
					List<Value> itemList = new ArrayList<Value>();
					itemList.add(Value.get(55));
					itemList.add(Value.get(77));

					return reactorClient.operate(key,
							ListOperation.appendItems(binName, itemList),
							ListOperation.pop(binName, -1),
							ListOperation.size(binName));
				});

		StepVerifier.create(mono)
				.expectNextMatches(keyRecord -> {
					assertRecordFound(keyRecord.key, keyRecord.record);

					List<?> list = keyRecord.record.getList(binName);

					long size = (Long)list.get(0);
					assertEquals(2, size);

					long val = (Long)list.get(1);
					assertEquals(77, val);

					size = (Long)list.get(2);
					assertEquals(1, size);
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void operateMap() {
		final Key key = new Key(args.namespace, args.set, "aopmkey1");

		Mono<KeyRecord> mono = reactorClient.delete(key)
				.switchIfEmpty(Mono.just(key))
				.flatMap(key1 -> {
					Map<Value,Value> map = new HashMap<Value,Value>();
					map.put(Value.get("a"), Value.get(1));
					map.put(Value.get("b"), Value.get(2));
					map.put(Value.get("c"), Value.get(3));

					return reactorClient.operate(key,
							MapOperation.putItems(MapPolicy.Default, binName, map),
							MapOperation.getByRankRange(binName, -1, 1, MapReturnType.KEY_VALUE));
				});

		StepVerifier.create(mono)
				.expectNextMatches(keyRecord -> {
					assertRecordFound(keyRecord.key, keyRecord.record);

					List<?> results = keyRecord.record.getList(binName);

					long size = (Long)results.get(0);
					assertEquals(3, size);

					List<?> list = (List<?>)results.get(1);
					Entry<?,?> entry = (Entry<?,?>)list.get(0);
					assertEquals("c", entry.getKey());
					assertEquals(3L, entry.getValue());
					return true;
				})
				.verifyComplete();
	}

}
