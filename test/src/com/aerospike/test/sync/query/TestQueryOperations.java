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
package com.aerospike.test.sync.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.util.Version;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

public class TestQueryOperations extends TestSync {
	private static final String indexName = "tqoindex";
	private static final String keyPrefix = "tqokey";
	private static final String binName1 = "tqobin1";
	private static final String binName2 = "tqobin2";
	private static final String binName3 = "tqobin3";
	private static final String mapBin = "tqomapbin";
	private static final int size = 20;

	@BeforeClass
	public static void prepare() {
		try {
			IndexTask itask = client.createIndex(args.indexPolicy, args.namespace, args.set, indexName, binName1, IndexType.NUMERIC);
			itask.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, args.set, keyPrefix + i);
			Map<Value, Value> map = new HashMap<>();
			map.put(Value.get("a"), Value.get(i));
			map.put(Value.get("b"), Value.get(i * 10));
			client.put(null, key,
				new Bin(binName1, i),
				new Bin(binName2, i * 10),
				new Bin(binName3, i * 100),
				new Bin(mapBin, map));
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, args.set, indexName);
	}

	@Test
	public void queryProjectMultipleBins() {
		Assume.assumeTrue("Ops projection extended requires server version 8.1.2 or later",
			client.getCluster().getRandomNode().getServerVersion().isGreaterOrEqual(8, 1, 2, 0));

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);

		stmt.setOperations(new Operation[] {
			Operation.get(binName1),
			Operation.get(binName2),
			MapOperation.getByKey(mapBin, Value.get("a"), MapReturnType.VALUE)
		});

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				assertNotNull(record.getValue(binName1));
				assertNotNull(record.getValue(binName2));
				assertNotNull(record.getValue(mapBin));

				long val1 = record.getLong(binName1);
				long val2 = record.getLong(binName2);
				long mapVal = record.getLong(mapBin);
				assertEquals(val1 * 10, val2);
				assertEquals(val1, mapVal);
				assertNull(record.getValue(binName3));
				count++;
			}
			assertTrue(count >= size);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryProjectSubsetOfBins() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		stmt.setOperations(new Operation[] {
			Operation.get(binName1),
			Operation.get(binName3)
		});

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				long val1 = record.getLong(binName1);
				long val3 = record.getLong(binName3);
				assertTrue(val1 >= begin && val1 <= end);
				assertEquals(val1 * 100, val3);
				assertNull(record.getValue(binName2));
				count++;
			}
			assertEquals(end - begin + 1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryProjectBinsViaExpressionRead() {
		Assume.assumeTrue("Ops projection extended requires server version 8.1.2 or later",
			client.getCluster().getRandomNode().getServerVersion().isGreaterOrEqual(8, 1, 2, 0));

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);

		Expression exp1 = Exp.build(Exp.intBin(binName1));
		Expression exp2 = Exp.build(Exp.intBin(binName2));
		Expression exp3 = Exp.build(Exp.intBin(binName3));

		stmt.setOperations(new Operation[] {
			ExpOperation.read("result1", exp1, ExpReadFlags.DEFAULT),
			ExpOperation.read("result2", exp2, ExpReadFlags.DEFAULT),
			ExpOperation.read("result3", exp3, ExpReadFlags.DEFAULT)
		});

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				long r1 = record.getLong("result1");
				long r2 = record.getLong("result2");
				long r3 = record.getLong("result3");
				assertEquals(r1 * 10, r2);
				assertEquals(r1 * 100, r3);
				count++;
			}
			assertTrue(count >= size);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryProjectBinsViaExpressionReadWithFilter() {
		Assume.assumeTrue("Ops projection extended requires server version 8.1.2 or later",
			client.getCluster().getRandomNode().getServerVersion().isGreaterOrEqual(8, 1, 2, 0));

		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		Expression exp1 = Exp.build(Exp.intBin(binName1));
		Expression exp2 = Exp.build(Exp.intBin(binName2));
		Expression exp3 = Exp.build(Exp.intBin(binName3));

		stmt.setOperations(new Operation[] {
			ExpOperation.read("result1", exp1, ExpReadFlags.DEFAULT),
			ExpOperation.read("result2", exp2, ExpReadFlags.DEFAULT),
			ExpOperation.read("result3", exp3, ExpReadFlags.DEFAULT)
		});

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				long r1 = record.getLong("result1");
				long r2 = record.getLong("result2");
				long r3 = record.getLong("result3");
				assertTrue(r1 >= begin && r1 <= end);
				assertEquals(r1 * 10, r2);
				assertEquals(r1 * 100, r3);
				count++;
			}
			assertEquals(end - begin + 1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryProjectMixedGetAndExpressionRead() {
		Assume.assumeTrue("Ops projection extended requires server version 8.1.2 or later",
			client.getCluster().getRandomNode().getServerVersion().isGreaterOrEqual(8, 1, 2, 0));

		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		Expression computedExp = Exp.build(
			Exp.add(Exp.intBin(binName1), Exp.intBin(binName2))
		);

		stmt.setOperations(new Operation[] {
			Operation.get(binName1),
			ExpOperation.read("sum", computedExp, ExpReadFlags.DEFAULT)
		});

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				long val1 = record.getLong(binName1);
				long sum = record.getLong("sum");
				assertTrue(val1 >= begin && val1 <= end);
				assertEquals(val1 + val1 * 10, sum);
				assertNull(record.getValue(binName2));
				assertNull(record.getValue(binName3));
				count++;
			}
			assertEquals(end - begin + 1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryWithExpReadOperation() {
		Assume.assumeTrue("Ops projection extended requires server version 8.1.2 or later",
			client.getCluster().getRandomNode().getServerVersion().isGreaterOrEqual(8, 1, 2, 0));

		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		Expression exp = Exp.build(
			Exp.mul(Exp.intBin(binName1), Exp.val(100))
		);

		stmt.setOperations(new Operation[] {
			Operation.get(binName1),
			ExpOperation.read("computed", exp, ExpReadFlags.DEFAULT)
		});

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				long computed = record.getLong("computed");
				long original = record.getLong(binName1);
				assertEquals(original * 100, computed);
				count++;
			}
			assertEquals(end - begin + 1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryWithMultipleExpReadOperations() {
		Assume.assumeTrue("Ops projection extended requires server version 8.1.2 or later",
			client.getCluster().getRandomNode().getServerVersion().isGreaterOrEqual(8, 1, 2, 0));

		int begin = 5;
		int end = 15;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		Expression sumExp = Exp.build(
			Exp.add(Exp.intBin(binName1), Exp.intBin(binName2))
		);
		Expression diffExp = Exp.build(
			Exp.sub(Exp.intBin(binName2), Exp.intBin(binName1))
		);

		stmt.setOperations(new Operation[] {
			Operation.get(binName1),
			Operation.get(binName2),
			ExpOperation.read("sum", sumExp, ExpReadFlags.DEFAULT),
			ExpOperation.read("diff", diffExp, ExpReadFlags.DEFAULT)
		});

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				long val1 = record.getLong(binName1);
				long val2 = record.getLong(binName2);
				long sum = record.getLong("sum");
				long diff = record.getLong("diff");
				assertEquals(val1 + val2, sum);
				assertEquals(val2 - val1, diff);
				count++;
			}
			assertEquals(end - begin + 1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryWithExpReadAndFilterExp() {
		Assume.assumeTrue("Ops projection extended requires server version 8.1.2 or later",
			client.getCluster().getRandomNode().getServerVersion().isGreaterOrEqual(8, 1, 2, 0));

		int begin = 1;
		int end = 20;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		Expression computedExp = Exp.build(
			Exp.mul(Exp.intBin(binName1), Exp.val(2))
		);

		stmt.setOperations(new Operation[] {
			Operation.get(binName1),
			ExpOperation.read("doubled", computedExp, ExpReadFlags.DEFAULT)
		});

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.lt(Exp.intBin(binName1), Exp.val(6))
		);

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				long doubled = record.getLong("doubled");
				long original = record.getLong(binName1);
				assertEquals(original * 2, doubled);
				assertTrue(original < 6);
				count++;
			}
			assertEquals(5, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryWithGetOperation() {
		int begin = 1;
		int end = 5;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		stmt.setOperations(new Operation[] { Operation.get(binName1) });

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				long val1 = record.getLong(binName1);
				assertTrue(val1 >= begin && val1 <= end);
				assertNull(record.getValue(binName2));
				count++;
			}
			assertEquals(end - begin + 1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryRejectsWriteOperation() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 1, 5));

		Bin bin = new Bin("foo", "bar");
		stmt.setOperations(new Operation[] { Operation.put(bin) });

		try {
			RecordSet rs = client.query(null, stmt);

			while (rs.next()) {
			}
			rs.close();
			fail("Expected AerospikeException for write operation in foreground query");
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertTrue(ae.getMessage().contains("read-only"));
		}
	}

	@Test
	public void queryRejectsExpWriteOperation() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 1, 5));

		Expression exp = Exp.build(Exp.val("bar"));
		stmt.setOperations(new Operation[] { ExpOperation.write("foo", exp, ExpWriteFlags.DEFAULT) });

		try {
			RecordSet rs = client.query(null, stmt);

			while (rs.next()) {
			}
			rs.close();
			fail("Expected AerospikeException for ExpWrite in foreground query");
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertTrue(ae.getMessage().contains("read-only"));
		}
	}

	@Test
	public void queryRejectsMixedReadWriteOperations() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 1, 5));

		Expression readExp = Exp.build(Exp.intBin(binName1));
		Expression writeExp = Exp.build(Exp.val("updated"));

		stmt.setOperations(new Operation[] {
			ExpOperation.read("computed", readExp, ExpReadFlags.DEFAULT),
			ExpOperation.write("foo", writeExp, ExpWriteFlags.DEFAULT)
		});

		try {
			RecordSet rs = client.query(null, stmt);

			while (rs.next()) {
			}
			rs.close();
			fail("Expected AerospikeException for mixed ops in foreground query");
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			Version serverVersion = client.getCluster().getRandomNode().getServerVersion();
			if (serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1_2)) {
				assertTrue(ae.getMessage().contains("read-only"));
			}
			else {
				assertTrue(ae.getMessage().contains("basic read operations"));
			}
		}
	}

	@Test
	public void executeRejectsReadOnlyOperations() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 1, 5));

		Expression exp = Exp.build(Exp.intBin(binName1));

		try {
			client.execute(null, stmt, ExpOperation.read("computed", exp, ExpReadFlags.DEFAULT));
			fail("Expected AerospikeException for read-only ops in background execute");
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertTrue(ae.getMessage().contains("write"));
		}
	}

	@Test
	public void executeWithWriteOperationSucceeds() {
		int begin = 1;
		int end = 3;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		Expression exp = Exp.build(Exp.val("executed"));
		ExecuteTask task = client.execute(null, stmt,
			ExpOperation.write("marker", exp, ExpWriteFlags.DEFAULT)
		);
		task.waitTillComplete(3000, 3000);

		for (int i = begin; i <= end; i++) {
			Key key = new Key(args.namespace, args.set, keyPrefix + i);
			Record record = client.get(null, key);
			assertNotNull(record);
			assertEquals("executed", record.getString("marker"));
		}
	}

	@Test
	public void executeRejectsMixedReadWriteOperations() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 1, 5));

		Expression readExp = Exp.build(Exp.intBin(binName1));
		Expression writeExp = Exp.build(Exp.val("mixed"));

		try {
			client.execute(null, stmt,
				ExpOperation.read("computed", readExp, ExpReadFlags.DEFAULT),
				ExpOperation.write("tag", writeExp, ExpWriteFlags.DEFAULT)
			);
			fail("Expected AerospikeException for mixed read/write ops in background execute");
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertTrue(ae.getMessage().contains("write-only"));
		}
	}

	@Test
	public void queryWithExpReadNoFilter() {
		Assume.assumeTrue("Ops projection extended requires server version 8.1.2 or later",
			client.getCluster().getRandomNode().getServerVersion().isGreaterOrEqual(8, 1, 2, 0));

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);

		Expression exp = Exp.build(
			Exp.add(Exp.intBin(binName1), Exp.val(1000))
		);

		stmt.setOperations(new Operation[] { ExpOperation.read("offset", exp, ExpReadFlags.DEFAULT) });

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				Object offsetVal = record.getValue("offset");
				assertNotNull(offsetVal);
				count++;
			}
			assertTrue(count >= size);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryWithExpReadConditional() {
		Assume.assumeTrue("Ops projection extended requires server version 8.1.2 or later",
			client.getCluster().getRandomNode().getServerVersion().isGreaterOrEqual(8, 1, 2, 0));

		int begin = 1;
		int end = 20;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		Expression exp = Exp.build(
			Exp.cond(
				Exp.gt(Exp.intBin(binName1), Exp.val(10)), Exp.val("high"),
				Exp.val("low")
			)
		);

		stmt.setOperations(new Operation[] {
			Operation.get(binName1),
			ExpOperation.read("category", exp, ExpReadFlags.DEFAULT)
		});

		RecordSet rs = client.query(null, stmt);

		try {
			int highCount = 0;
			int lowCount = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				String category = record.getString("category");
				long val = record.getLong(binName1);
				assertNotNull(category);

				if (val > 10) {
					assertEquals("high", category);
					highCount++;
				}
				else {
					assertEquals("low", category);
					lowCount++;
				}
			}
			assertEquals(10, highCount);
			assertEquals(10, lowCount);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryRejectsTouchOperation() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 1, 5));

		stmt.setOperations(new Operation[] { Operation.touch() });

		try {
			RecordSet rs = client.query(null, stmt);

			while (rs.next()) {
			}
			rs.close();
			fail("Expected AerospikeException for Touch in foreground query");
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertTrue(ae.getMessage().contains("read-only"));
		}
	}

	@Test
	public void queryRejectsDeleteOperation() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 1, 5));

		stmt.setOperations(new Operation[] { Operation.delete() });

		try {
			RecordSet rs = client.query(null, stmt);

			while (rs.next()) {
			}
			rs.close();
			fail("Expected AerospikeException for Delete in foreground query");
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertTrue(ae.getMessage().contains("read-only"));
		}
	}

	@Test
	public void queryWithExpReadEvalNoFail() {
		Assume.assumeTrue("Ops projection extended requires server version 8.1.2 or later",
			client.getCluster().getRandomNode().getServerVersion().isGreaterOrEqual(8, 1, 2, 0));

		int begin = 1;
		int end = 5;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		Expression exp = Exp.build(Exp.intBin("nonexistent"));
		stmt.setOperations(new Operation[] {
			Operation.get(binName1),
			ExpOperation.read("result", exp, ExpReadFlags.EVAL_NO_FAIL)
		});

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				assertNotNull(record.getValue(binName1));
				count++;
			}
			assertEquals(end - begin + 1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void executeRejectsGetOperation() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 1, 5));

		try {
			client.execute(null, stmt, Operation.get(binName1));
			fail("Expected AerospikeException for read-only Get in background execute");
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertTrue(ae.getMessage().contains("write"));
		}
	}

	@Test
	public void queryOperationsTakePrecedenceOverBinNames() {
		// When both binNames and operations are set, operations take precedence
		// (Java client logs a warning but does not throw).
		int begin = 1;
		int end = 5;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));
		stmt.setBinNames(binName1, binName2, binName3);
		stmt.setOperations(new Operation[] { Operation.get(binName1) });

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				long val1 = record.getLong(binName1);
				assertTrue(val1 >= begin && val1 <= end);
				// Operations projected only binName1, so binName2/binName3 should be absent.
				assertNull(record.getValue(binName2));
				assertNull(record.getValue(binName3));
				count++;
			}
			assertEquals(end - begin + 1, count);
		}
		finally {
			rs.close();
		}
	}
}
