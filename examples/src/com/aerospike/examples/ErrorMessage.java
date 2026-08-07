/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.examples;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Value;

import java.util.ArrayList;
import java.util.List;

public class ErrorMessage extends Example {

	@Override
	public void runExample() throws Exception {
		IAerospikeClient client = client();
		Parameters params = params();
		String binName = "test-bin";
		Key key = new Key(params.namespace, params.set, "error-message-key");

		// Write a record with an integer bin.
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.errorDetailVerbosity = 2;
		client.put(writePolicy, key, new Bin(binName, 1));
		console.info("Write succeeded, running error detail tests.");

		// Test 1: Append string to integer bin.
		testAppendToIntegerBin(client, params, writePolicy, key, binName);

		// Test 2: Delete with wrong generation.
		testDeleteGenerationMismatch(client, params, key);

		// Test 3: Increment a string bin.
		testIncrementStringBin(client, params, writePolicy, binName);

		// Test 4: HLL add on integer bin.
		testHllAddOnIntegerBin(client, params, writePolicy, key, binName);

		// Test 5: HLL refresh_count on nonexistent bin.
		testHllRefreshCountMissingBin(client, params, writePolicy);

		console.info("Error message example completed successfully.");
	}

	private void testAppendToIntegerBin(
		IAerospikeClient client,
		Parameters params,
		WritePolicy writePolicy,
		Key key,
		String binName
	) {
		try {
			client.operate(writePolicy, key, Operation.append(new Bin(binName, "bad-append")));
			throw new RuntimeException("Expected error on append to integer bin");
		}
		catch (AerospikeException ae) {
			assertErrorDetails(ae, ResultCode.BIN_TYPE_ERROR, "cannot append", "subcode=1100");
			console.info("Test 1 passed: append to integer bin - %d: %s", ae.getResultCode(), ae.getBaseMessage());
		}
	}

	private void testDeleteGenerationMismatch(IAerospikeClient client, Parameters params, Key key) {
		WritePolicy rmPolicy = new WritePolicy();
		rmPolicy.errorDetailVerbosity = 2;
		rmPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		rmPolicy.generation = 777;

		try {
			client.delete(rmPolicy, key);
			throw new RuntimeException("Expected error on generation-mismatch delete");
		}
		catch (AerospikeException ae) {
			assertErrorDetails(ae, ResultCode.GENERATION_ERROR, "delete generation mismatch", "subcode=1701");
			console.info("Test 2 passed: generation mismatch delete - %d: %s", ae.getResultCode(), ae.getBaseMessage());
		}
	}

	private void testIncrementStringBin(
		IAerospikeClient client,
		Parameters params,
		WritePolicy writePolicy,
		String binName
	) {
		Key key2 = new Key(params.namespace, params.set, "error-message-key-2");
		client.put(writePolicy, key2, new Bin(binName, "hello"));

		try {
			client.operate(writePolicy, key2, Operation.add(new Bin(binName, 1)));
			throw new RuntimeException("Expected error on incr of string bin");
		}
		catch (AerospikeException ae) {
			assertErrorDetails(ae, ResultCode.BIN_TYPE_ERROR, "cannot increment", "subcode=1100");
			console.info("Test 3 passed: increment string bin - %d: %s", ae.getResultCode(), ae.getBaseMessage());
		}
	}

	private void testHllAddOnIntegerBin(
		IAerospikeClient client,
		Parameters params,
		WritePolicy writePolicy,
		Key key,
		String binName
	) {
		List<Value> hllList = new ArrayList<>();
		hllList.add(Value.get("element1"));

		try {
			client.operate(writePolicy, key,
				HLLOperation.add(HLLPolicy.Default, binName, hllList, 8));
			throw new RuntimeException("Expected error on HLL add to integer bin");
		}
		catch (AerospikeException ae) {
			assertErrorDetails(ae, ResultCode.BIN_TYPE_ERROR, "bin is not hll type", "subcode=1138");
			console.info("Test 4 passed: HLL add on integer bin - %d: %s", ae.getResultCode(), ae.getBaseMessage());
		}
	}

	private void testHllRefreshCountMissingBin(
		IAerospikeClient client,
		Parameters params,
		WritePolicy writePolicy
	) {
		Key key3 = new Key(params.namespace, params.set, "error-message-key-3");
		client.put(writePolicy, key3, new Bin("other-bin", 1));

		try {
			client.operate(writePolicy, key3,
				HLLOperation.refreshCount("no-hll-bin"));
			throw new RuntimeException("Expected error on HLL refresh_count of nonexistent bin");
		}
		catch (AerospikeException ae) {
			assertErrorDetails(ae, ResultCode.BIN_NOT_FOUND, "subcode=1134");
			console.info("Test 5 passed: HLL refresh_count missing bin - %d: %s", ae.getResultCode(), ae.getBaseMessage());
		}
	}

	private void assertErrorDetails(AerospikeException ae, int expectedResultCode, String... expectedSubstrings) {
		if (ae.getResultCode() != expectedResultCode) {
			throw new RuntimeException(
				"Expected result code " + expectedResultCode + " but got " + ae.getResultCode() + ": " + ae.getBaseMessage());
		}

		String msg = ae.getBaseMessage();

		for (String expected : expectedSubstrings) {
			if (!msg.contains(expected)) {
				throw new RuntimeException(
					"Expected '" + expected + "' in error message: " + msg);
			}
		}
	}
}
