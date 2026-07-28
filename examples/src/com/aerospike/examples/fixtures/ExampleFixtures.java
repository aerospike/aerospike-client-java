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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Statement;
import com.aerospike.examples.ExampleFixture;
import com.aerospike.examples.Parameters;

public final class ExampleFixtures {
	private static final String ADD_KEY = "addkey";
	private static final String ADD_BIN = "addbin";
	private static final String DELETE_BIN_KEY = "delbinkey";
	private static final String PUT_GET_KEY = "putgetkey";
	private static final String REPLACE_KEY = "replacekey";
	private static final String REPLACE_ONLY_KEY = "replaceonlykey";
	private static final String APPEND_KEY = "appendkey";
	private static final String PREPEND_KEY = "prependkey";
	private static final String GENERATION_KEY = "genkey";
	private static final String EXPIRE_KEY = "expirekey ";
	private static final String TOUCH_KEY = "touchkey";
	private static final String OPERATE_KEY = "opkey";
	private static final String OPERATE_BIT_KEY = "bitkey";
	private static final String QUERY_INTEGER_INDEX = "queryindexint";
	private static final String QUERY_INTEGER_KEY_PREFIX = "querykeyint";
	private static final String QUERY_INTEGER_BIN = "querybinint";
	private static final int QUERY_INTEGER_SIZE = 50;
	private static final String UDF_PACKAGE_PATH = "udf/record_example.lua";
	private static final String UDF_FILE = "record_example.lua";
	private static final String ASYNC_BATCH_KEY_PREFIX = "batchkey";
	private static final String ASYNC_BATCH_BIN = "batchbin";

	private ExampleFixtures() {
	}

	public static ExampleFixture putGetExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, PUT_GET_KEY);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Key key = FixtureSupport.key(params, PUT_GET_KEY);
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), key, "bin1", "value1");
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), key, "bin2", "value2");
				ExampleAssertions.assertHeaderHasGeneration(client.getHeader(params.readPolicy(), key), key);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, PUT_GET_KEY);
			}
		};
	}

	public static ExampleFixture replaceExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, REPLACE_KEY, REPLACE_ONLY_KEY);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Key replaceKey = FixtureSupport.key(params, REPLACE_KEY);
				ExampleAssertions.assertRecordBinsDeepEquals(
					ExampleAssertions.assertRecordExists(client, params.readPolicy(), replaceKey),
					replaceKey,
					mapOf("bin3", "value3"));
				ExampleAssertions.assertRecordMissing(client, params.readPolicy(), FixtureSupport.key(params, REPLACE_ONLY_KEY));
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, REPLACE_KEY, REPLACE_ONLY_KEY);
			}
		};
	}

	public static ExampleFixture addExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, ADD_KEY);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertBinEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, ADD_KEY),
					ADD_BIN,
					45L);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, ADD_KEY);
			}
		};
	}

	public static ExampleFixture appendExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, APPEND_KEY);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertBinEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, APPEND_KEY),
					"appendbin",
					"Hello World");
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, APPEND_KEY);
			}
		};
	}

	public static ExampleFixture prependExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, PREPEND_KEY);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertBinEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, PREPEND_KEY),
					"prependbin",
					"Hello World");
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, PREPEND_KEY);
			}
		};
	}

	public static ExampleFixture deleteBinExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				Key key = FixtureSupport.key(params, DELETE_BIN_KEY);
				FixtureSupport.deleteKeys(client, params, DELETE_BIN_KEY);
				client.put(params.writePolicy(), key, new Bin("bin1", "value1"), new Bin("bin2", "value2"));
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Key key = FixtureSupport.key(params, DELETE_BIN_KEY);
				ExampleAssertions.assertBinRemoved(client, params.readPolicy(), key, "bin1");
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), key, "bin2", "value2");
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, DELETE_BIN_KEY);
			}
		};
	}

	public static ExampleFixture generationExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, GENERATION_KEY);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertBinEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, GENERATION_KEY),
					"genbin",
					"genvalue3");
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, GENERATION_KEY);
			}
		};
	}

	public static ExampleFixture expireExample() {
		return missingRecordFixture(EXPIRE_KEY);
	}

	public static ExampleFixture touchExample() {
		return missingRecordFixture(TOUCH_KEY);
	}

	public static ExampleFixture operateExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, OPERATE_KEY);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Key key = FixtureSupport.key(params, OPERATE_KEY);
				ExampleAssertions.assertRecordBinsDeepEquals(
					ExampleAssertions.assertRecordExists(client, params.readPolicy(), key),
					key,
					mapOf(
						"bin1", 11L,
						"bin2", "new string",
						"bin3", 77.7,
						"bin4", "bin4val"));
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, OPERATE_KEY);
			}
		};
	}

	public static ExampleFixture operateBitExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, OPERATE_BIT_KEY);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertBlobEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, OPERATE_BIT_KEY),
					"bitbin",
					new byte[] {0b00000001, 0b00000010, 0b00000011, 0b00000100, 0b00000111});
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, OPERATE_BIT_KEY);
			}
		};
	}

	public static ExampleFixture queryIntegerExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.createIndexIfMissing(
					client,
					params,
					params.set(),
					QUERY_INTEGER_INDEX,
					QUERY_INTEGER_BIN,
					IndexType.NUMERIC);
				FixtureSupport.deleteKeyRange(client, params, QUERY_INTEGER_KEY_PREFIX, 1, QUERY_INTEGER_SIZE);
				FixtureSupport.seedIntegerRange(client, params, QUERY_INTEGER_KEY_PREFIX, QUERY_INTEGER_BIN, QUERY_INTEGER_SIZE);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertQueryCount(client, params.readPolicy(), queryIntegerStatement(params), 5);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, QUERY_INTEGER_INDEX);
				FixtureSupport.deleteKeyRange(client, params, QUERY_INTEGER_KEY_PREFIX, 1, QUERY_INTEGER_SIZE);
			}
		};
	}

	public static ExampleFixture userDefinedFunctionExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.registerLua(client, params, UDF_PACKAGE_PATH, UDF_FILE);
				FixtureSupport.deleteKeys(client, params, "udfkey1", "udfkey2", "udfkey3", "udfkey4", "udfkey5", "udfkey6");
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) throws Exception {
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, "udfkey1"), "udfbin1", "string value");
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, "udfkey2"), "udfbin2", "string value");
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, "udfkey3"), "udfbin3", "first");
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, "udfkey4"), "udfbin4", 4L);
				ExampleAssertions.assertListTailEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, "udfkey5"),
					"udfbin5",
					"appended value");
				ExampleAssertions.assertBlobEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, "udfkey6"),
					"udfbin6",
					createExpectedBlob());
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, "udfkey1", "udfkey2", "udfkey3", "udfkey4", "udfkey5", "udfkey6");
			}
		};
	}

	public static ExampleFixture asyncPutGetExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, PUT_GET_KEY);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertBinEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, PUT_GET_KEY),
					"putgetbin",
					"value");
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, PUT_GET_KEY);
			}
		};
	}

	public static ExampleFixture asyncBatchExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				deleteAsyncBatchKeys(client, params);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				for (int i = 1; i <= 8; i++) {
					ExampleAssertions.assertBinEquals(
						client,
						params.readPolicy(),
						FixtureSupport.key(params, ASYNC_BATCH_KEY_PREFIX + i),
						ASYNC_BATCH_BIN,
						"batchvalue" + i);
				}
				ExampleAssertions.assertRecordMissing(client, params.readPolicy(), FixtureSupport.key(params, "keynotfound"));
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				deleteAsyncBatchKeys(client, params);
			}
		};
	}

	public static ExampleFixture asyncUserDefinedFunctionExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.registerLua(client, params, UDF_PACKAGE_PATH, UDF_FILE);
				FixtureSupport.deleteKeys(client, params, "audfkey1");
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertBinEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, "audfkey1"),
					"audfbin1",
					"string value");
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, "audfkey1");
			}
		};
	}

	private static void deleteAsyncBatchKeys(IAerospikeClient client, Parameters params) {
		Object[] keys = new Object[9];

		for (int i = 1; i <= 8; i++) {
			keys[i - 1] = ASYNC_BATCH_KEY_PREFIX + i;
		}
		keys[8] = "keynotfound";
		FixtureSupport.deleteKeys(client, params, keys);
	}

	private static ExampleFixture missingRecordFixture(String userKey) {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, userKey);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertRecordMissing(client, params.readPolicy(), FixtureSupport.key(params, userKey));
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) {
				FixtureSupport.deleteKeys(client, params, userKey);
			}
		};
	}

	private static Statement queryIntegerStatement(Parameters params) {
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace());
		stmt.setSetName(params.set());
		stmt.setBinNames(QUERY_INTEGER_BIN);
		stmt.setFilter(Filter.range(QUERY_INTEGER_BIN, 14, 18));
		return stmt;
	}

	private static byte[] createExpectedBlob() throws Exception {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (DataOutputStream dos = new DataOutputStream(baos)) {
				dos.writeInt(9845);
				dos.writeUTF("Hello world.");
			}
			return baos.toByteArray();
		}
	}

	private static Map<String,Object> mapOf(Object... entries) {
		Map<String,Object> map = new LinkedHashMap<>();

		for (int i = 0; i < entries.length; i += 2) {
			map.put((String)entries[i], entries[i + 1]);
		}
		return map;
	}
}
