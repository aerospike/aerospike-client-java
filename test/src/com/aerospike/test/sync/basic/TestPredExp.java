/*
 * Copyright 2012-2020 Aerospike, Inc.
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

package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.test.sync.TestSync;

public class TestPredExp extends TestSync {
	Key keyA = new Key(args.namespace, args.set, "A");
	Key keyB = new Key(args.namespace, args.set, "B");

	String binAName = "A";

	Bin binA1 = new Bin(binAName, 1l);
	Bin binA2 = new Bin(binAName, 2l);
	Bin binA3 = new Bin(binAName, 3l);

	BatchPolicy predAEq1BPolicy;
	Policy predAEq1RPolicy;
	WritePolicy predAEq1WPolicy;

	@BeforeClass
	public static void register() {
		RegisterTask task = client.register(null,
				TestUDF.class.getClassLoader(), "udf/record_example.lua",
				"record_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	@Before
	public void setUp() throws Exception {
		predAEq1BPolicy = new BatchPolicy();
		predAEq1RPolicy = new Policy();
		predAEq1WPolicy = new WritePolicy();

		predAEq1BPolicy.setPredExp(
				PredExp.integerBin(binAName),
				PredExp.integerValue(1),
				PredExp.integerEqual());

		predAEq1RPolicy.setPredExp(
				PredExp.integerBin(binAName),
				PredExp.integerValue(1),
				PredExp.integerEqual());

		predAEq1WPolicy.setPredExp(
				PredExp.integerBin(binAName),
				PredExp.integerValue(1),
				PredExp.integerEqual());

		client.delete(null, keyA);
		client.delete(null, keyB);

		client.put(null, keyA, binA1);
		client.put(null, keyB, binA2);
	}

	@Test
	public void put() {
		client.put(predAEq1WPolicy, keyA, binA3);
		Record r = client.get(null, keyA);

		assertBinEqual(keyA, r, binA3);

		client.put(predAEq1WPolicy, keyB, binA3);
		r = client.get(null, keyB);

		assertBinEqual(keyB, r, binA2);
	}

	@Test
	public void putExcept() {
		predAEq1WPolicy.failOnFilteredOut = true;

		client.put(predAEq1WPolicy, keyA, binA3);

		try {
			client.put(predAEq1WPolicy, keyB, binA3);
			fail("Expected AerospikeException filtered out (27)");
		}
		catch (AerospikeException e) {
			assertEquals(27, e.getResultCode());
		}
	}

	@Test
	public void get() {
		Record r = client.get(predAEq1RPolicy, keyA);

		assertBinEqual(keyA, r, binA1);

		r = client.get(predAEq1RPolicy, keyB);

		assertEquals(null, r);
	}

	@Test
	public void getExcept() {
		predAEq1RPolicy.failOnFilteredOut = true;

		client.get(predAEq1RPolicy, keyA);

		try {
			client.get(predAEq1RPolicy, keyB);
			fail("Expected AerospikeException filtered out (27)");
		}
		catch (AerospikeException e) {
			assertEquals(27, e.getResultCode());
		}
	}

	@Test
	public void batch() {
		Key[] keys = { keyA, keyB };

		Record[] records = client.get(predAEq1BPolicy, keys);

		assertBinEqual(keyA, records[0], binA1);
		assertEquals(null, records[1]);
	}

	@Test
	public void delete() {
		client.delete(predAEq1WPolicy, keyA);
		Record r = client.get(null, keyA);

		assertEquals(null, r);

		client.delete(predAEq1WPolicy, keyB);
		r = client.get(null,  keyB);

		assertBinEqual(keyB, r, binA2);
	}

	@Test
	public void deleteExcept() {
		predAEq1WPolicy.failOnFilteredOut = true;

		client.delete(predAEq1WPolicy, keyA);

		try {
			client.delete(predAEq1WPolicy, keyB);
		}
		catch (AerospikeException e) {
			assertEquals(27, e.getResultCode());
		}
	}

	@Test
	public void durableDelete() {
		predAEq1WPolicy.durableDelete = true;

		client.delete(predAEq1WPolicy, keyA);
		Record r = client.get(null, keyA);

		assertEquals(null, r);

		client.delete(predAEq1WPolicy, keyB);
		r = client.get(null,  keyB);

		assertBinEqual(keyB, r, binA2);
	}

	@Test
	public void durableDeleteExcept() {
		predAEq1WPolicy.failOnFilteredOut = true;
		predAEq1WPolicy.durableDelete = true;

		client.delete(predAEq1WPolicy, keyA);

		try {
			client.delete(predAEq1WPolicy, keyB);
			fail("Expected AerospikeException filtered out (27)");
		}
		catch (AerospikeException e) {
			assertEquals(27, e.getResultCode());
		}
	}

	@Test
	public void operateRead() {
		Record r = client.operate(predAEq1WPolicy, keyA,
				Operation.get(binAName));

		assertBinEqual(keyA, r, binA1);

		r = client.operate(predAEq1WPolicy, keyB,
				Operation.get(binAName));

		assertEquals(null, r);
	}

	@Test
	public void operateReadExcept() {
		predAEq1WPolicy.failOnFilteredOut = true;

		client.operate(predAEq1WPolicy, keyA, Operation.get(binAName));

		try {
			client.operate(predAEq1WPolicy, keyB, Operation.get(binAName));
			fail("Expected AerospikeException filtered out (27)");
		}
		catch (AerospikeException e) {
			assertEquals(27, e.getResultCode());
		}
	}

	@Test
	public void operateWrite() {
		Record r = client.operate(predAEq1WPolicy, keyA,
				Operation.put(binA3), Operation.get(binAName));

		assertBinEqual(keyA, r, binA3);

		r = client.operate(predAEq1WPolicy, keyB,
				Operation.put(binA3), Operation.get(binAName));

		assertEquals(null, r);
	}

	@Test
	public void operateWriteExcept() {
		predAEq1WPolicy.failOnFilteredOut = true;

		client.operate(predAEq1WPolicy, keyA,
				Operation.put(binA3), Operation.get(binAName));

		try {
			client.operate(predAEq1WPolicy, keyB,
					Operation.put(binA3), Operation.get(binAName));
			fail("Expected AerospikeException filtered out (27)");
		}
		catch (AerospikeException e) {
			assertEquals(27, e.getResultCode());
		}
	}

	@Test
	public void udf() {
		client.execute(predAEq1WPolicy, keyA, "record_example", "writeBin",
				Value.get(binA3.name), binA3.value);
		Record r = client.get(null, keyA);

		assertBinEqual(keyA, r, binA3);

		client.execute(predAEq1WPolicy, keyB, "record_example", "writeBin",
				Value.get(binA3.name), binA3.value);
		r = client.get(null, keyB);

		assertBinEqual(keyB, r, binA2);
	}

	@Test
	public void udfExcept() {
		predAEq1WPolicy.failOnFilteredOut = true;

		client.execute(predAEq1WPolicy, keyA, "record_example", "writeBin",
				Value.get(binA3.name), binA3.value);

		try {
			client.execute(predAEq1WPolicy, keyB, "record_example", "writeBin",
					Value.get(binA3.name), binA3.value);
			fail("Expected AerospikeException filtered out (27)");
		}
		catch (AerospikeException e) {
			assertEquals(27, e.getResultCode());
		}
	}
}
