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
package com.aerospike.examples.fixtures;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.examples.ExampleFixture;
import com.aerospike.examples.Parameters;

public final class AdvancedExampleFixtures {
	private AdvancedExampleFixtures() {
	}

	public static ExampleFixture operateListExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, "listkey", "listkey2");
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertBinDeepEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, "listkey"),
					"listbin",
					list(55L));
				ExampleAssertions.assertBinDeepEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, "listkey2"),
					"listbin",
					list(
						list(7L, 9L, 5L),
						list(1L, 2L, 3L),
						list(6L, 5L, 4L, 1L, 11L)));
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, "listkey", "listkey2");
			}
		};
	}

	public static ExampleFixture operateMapExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, "mapkey", "mapkey2", "mapkey3");
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				verifyMapKey(client, params);
				ExampleAssertions.assertBinDeepEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, "mapkey2"),
					"mapbin",
					mapOf(
						"key1", mapOf("key21", 7L, "key22", 6L),
						"key2", mapOf("a", 3L, "b", 4L, "c", 5L)));
				ExampleAssertions.assertBinDeepEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, "mapkey3"),
					"mapbin",
					mapOf("key1", list(7L, 9L, 5L), "key2", list(1L, 2L)));
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, "mapkey", "mapkey2", "mapkey3");
			}
		};
	}

	public static ExampleFixture batchOperateExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeyRange(client, params, "bkey", 1, 8);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertRecordMissing(client, params.readPolicy(), FixtureSupport.key(params, "bkey6"));
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, "bkey1"), "bin4", 100L);
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, "bkey4"), "bin1", 1018L);

				for (int i = 1; i <= 8; i++) {
					if (i == 6) {
						continue;
					}
					ExampleAssertions.assertListTailEquals(
						client,
						params.readPolicy(),
						FixtureSupport.key(params, "bkey" + i),
						"bin3",
						999L);
				}
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeyRange(client, params, "bkey", 1, 8);
			}
		};
	}

	public static ExampleFixture transactionExample() {
		return transactionFixture();
	}

	public static ExampleFixture asyncTransactionExample() {
		return transactionFixture();
	}

	public static ExampleFixture scanPageExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteNumericKeyRange(client, params, "page", 1, 190);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertSetRecordCount(
					client,
					null,
					params.namespace(),
					"page",
					190);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteNumericKeyRange(client, params, "page", 1, 190);
			}
		};
	}

	public static ExampleFixture scanResumeExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteNumericKeyRange(client, params, "resume", 1, 200);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertSetRecordCount(
					client,
					null,
					params.namespace(),
					"resume",
					200);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteNumericKeyRange(client, params, "resume", 1, 200);
			}
		};
	}

	public static ExampleFixture asyncScanPageExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteNumericKeyRange(client, params, "apage", 1, 50);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertSetRecordCount(
					client,
					null,
					params.namespace(),
					"apage",
					50);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteNumericKeyRange(client, params, "apage", 1, 50);
			}
		};
	}

	private static ExampleFixture transactionFixture() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.deleteKeys(client, params, 1, 2, 3);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, 1), "a", "val1");
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, 2), "b", "val2");
				ExampleAssertions.assertRecordMissing(client, params.readPolicy(), FixtureSupport.key(params, 3));
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, 1, 2, 3);
			}
		};
	}

	private static void verifyMapKey(IAerospikeClient client, Parameters params) {
		Key key = FixtureSupport.key(params, "mapkey");
		Record record = ExampleAssertions.assertRecordExists(client, params.readPolicy(), key, "mapbin");
		@SuppressWarnings("unchecked")
		Map<String,Object> map = (Map<String,Object>)record.getValue("mapbin");

		if (map.size() != 2 || ! map.containsKey("Harry") || ! map.containsKey("Bill")) {
			throw new IllegalStateException("Unexpected final mapkey contents: " + map);
		}

		long march2 = new GregorianCalendar(2018, 2, 2).getTimeInMillis();
		long march5 = new GregorianCalendar(2018, 2, 5).getTimeInMillis();
		ExampleAssertions.assertDeepEquals("mapkey Harry", list(march2, 4L), map.get("Harry"));
		ExampleAssertions.assertDeepEquals("mapkey Bill", list(march5, 5L), map.get("Bill"));
	}

	private static List<Object> list(Object... values) {
		List<Object> list = new ArrayList<Object>(values.length);

		for (Object value : values) {
			list.add(value);
		}
		return list;
	}

	private static Map<String,Object> mapOf(Object... entries) {
		Map<String,Object> map = new HashMap<String,Object>();

		for (int i = 0; i < entries.length; i += 2) {
			map.put((String)entries[i], entries[i + 1]);
		}
		return map;
	}
}
