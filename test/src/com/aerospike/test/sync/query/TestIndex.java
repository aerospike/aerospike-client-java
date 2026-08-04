/*
 * Copyright 2012-2024 Aerospike, Inc.
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

import org.junit.Assume;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.LoopVarPart;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.util.Version;
import com.aerospike.test.sync.TestSync;

public class TestIndex extends TestSync {
	private static final String indexName = "testindex";
	private static final String binName = "testbin";
	private static final String setIndexName = "testsetindex";
	private static final String integerIndexName = "testintegerindex";
	private static final String integerBinName = "testintegerbin";
	private static final String integerKeyPrefix = "testintegerkey";
	private static final String numericIndexName = "testnumericindex";
	private static final String numericBinName = "testnumericbin";
	private static final String numericKeyPrefix = "testnumerickey";

	@Test
	public void createDrop() {
		IndexTask task;

		// Drop index if it already exists.
		try {
			task = client.dropIndex(args.indexPolicy, args.namespace, args.set, indexName);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
				throw ae;
			}
		}

		task = client.createIndex(args.indexPolicy, args.namespace, args.set, indexName, binName, IndexType.NUMERIC);
		task.waitTillComplete();

		task = client.dropIndex(args.indexPolicy, args.namespace, args.set, indexName);
		task.waitTillComplete();

		// Ensure all nodes have dropped the index.
		Node[] nodes = client.getNodes();

		for (Node node : nodes) {
			String cmd = IndexTask.buildStatusCommand(args.namespace, indexName, node.serverVersion);
			String response = Info.request(node, cmd);
			int code = Info.parseResultCode(response);

			assertEquals(code, 201);
		}
	}

	@Test
	public void setIndexCreateDrop() {
		Version serverVersion = client.getCluster().getRandomNode().getServerVersion();
		Assume.assumeTrue("Set index tests require server version 8.1.2 or later",
			serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1_2));

		IndexTask task;

		// Drop set index if it already exists.
		try {
			task = client.dropIndex(args.indexPolicy, args.namespace, args.set, setIndexName);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
				throw ae;
			}
		}

		task = client.createIndex(args.indexPolicy, args.namespace, args.set, setIndexName);
		task.waitTillComplete();

		// Verify the set index exists on all nodes.
		Node[] nodes = client.getNodes();

		for (Node node : nodes) {
			String cmd = IndexTask.buildExistsCommand(args.namespace, setIndexName, node.serverVersion);
			String response = Info.request(node, cmd);
			assertEquals("true", response);
		}

		task = client.dropIndex(args.indexPolicy, args.namespace, args.set, setIndexName);
		task.waitTillComplete();

		// Ensure all nodes have dropped the set index.
		for (Node node : nodes) {
			String cmd = IndexTask.buildStatusCommand(args.namespace, setIndexName, node.serverVersion);
			String response = Info.request(node, cmd);
			int code = Info.parseResultCode(response);
			assertEquals(201, code);
		}
	}

	@Test
	public void integerIndexCreateQueryDrop() {
		Assume.assumeTrue("INTEGER index type requires server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));

		IndexTask task;

		// Drop index if it already exists.
		try {
			task = client.dropIndex(args.indexPolicy, args.namespace, args.set, integerIndexName);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
				throw ae;
			}
		}

		task = client.createIndex(args.indexPolicy, args.namespace, args.set, integerIndexName, integerBinName, IndexType.INTEGER);
		task.waitTillComplete();

		int size = 20;

		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, args.set, integerKeyPrefix + i);
			Bin bin = new Bin(integerBinName, i);
			client.put(null, key, bin);
		}

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(integerBinName);
		stmt.setFilter(Filter.range(integerBinName, 4, 8));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(5, count);
		}
		finally {
			rs.close();
		}

		task = client.dropIndex(args.indexPolicy, args.namespace, args.set, integerIndexName);
		task.waitTillComplete();

		// Ensure all nodes have dropped the index.
		Node[] nodes = client.getNodes();

		for (Node node : nodes) {
			String cmd = IndexTask.buildStatusCommand(args.namespace, integerIndexName, node.serverVersion);
			String response = Info.request(node, cmd);
			int code = Info.parseResultCode(response);

			assertEquals(201, code);
		}
	}

	@Test
	public void numericIndexUpgradesToIntegerQueryDrop() {
		// On server versions 8.1.3+ the client transparently upgrades a NUMERIC
		// request to the "integer" index type. The server collapses "numeric" and
		// "integer" to the same internal type, so the create spelling cannot be
		// read back; this test instead verifies the upgraded index is created and
		// remains queryable end-to-end. The wire-level mapping itself is asserted
		// by AerospikeClientIndexTypeTest.
		Assume.assumeTrue("NUMERIC to INTEGER upgrade requires server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));

		IndexTask task;

		// Drop index if it already exists.
		try {
			task = client.dropIndex(args.indexPolicy, args.namespace, args.set, numericIndexName);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
				throw ae;
			}
		}

		task = client.createIndex(args.indexPolicy, args.namespace, args.set, numericIndexName, numericBinName, IndexType.NUMERIC);
		task.waitTillComplete();

		int size = 20;

		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, args.set, numericKeyPrefix + i);
			Bin bin = new Bin(numericBinName, i);
			client.put(null, key, bin);
		}

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(numericBinName);
		stmt.setFilter(Filter.range(numericBinName, 4, 8));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(5, count);
		}
		finally {
			rs.close();
		}

		task = client.dropIndex(args.indexPolicy, args.namespace, args.set, numericIndexName);
		task.waitTillComplete();

		// Ensure all nodes have dropped the index.
		Node[] nodes = client.getNodes();

		for (Node node : nodes) {
			String cmd = IndexTask.buildStatusCommand(args.namespace, numericIndexName, node.serverVersion);
			String response = Info.request(node, cmd);
			int code = Info.parseResultCode(response);

			assertEquals(201, code);
		}
	}

	@Test
	public void ctxRestore() {
		CTX[] ctx1 = new CTX[] {
			CTX.listIndex(-1),
			CTX.mapKey(Value.get("key1")),
			CTX.listValue(Value.get(937))
		};

		String base64 = CTX.toBase64(ctx1);
		CTX[] ctx2 = CTX.fromBase64(base64);

		assertEquals(ctx1.length, ctx2.length);

		for (int i = 0; i < ctx1.length; i++) {
			CTX item1 = ctx1[i];
			CTX item2 = ctx2[i];

			assertEquals(item1.id, item2.id);

			Object obj1 = item1.value.getObject();
			Object obj2 = item2.value.getObject();

			if (obj1 instanceof Integer && obj2 instanceof Long) {
				// fromBase64() converts integers to long, so consider these equivalent.
				assertEquals((long)(Integer)obj1, (long)(Long)obj2);
			}
			else {
				assertEquals(obj1, obj2);
			}
		}
	}

	@Test
	public void allChildrenBase() {
		CTX[] ctx1 = new CTX[] {
			CTX.allChildren()
		};

		String base64 = CTX.toBase64(ctx1);
		CTX[] ctx2 = CTX.fromBase64(base64);

		assertEquals(ctx1.length, ctx2.length);
		
		CTX original = ctx1[0];
		CTX restored = ctx2[0];
		
		assertEquals(original.id, restored.id);
		assertEquals(original.exp.getBytes().length, restored.exp.getBytes().length);
		
		// Verify the expression bytes are equivalent
		byte[] originalBytes = original.exp.getBytes();
		byte[] restoredBytes = restored.exp.getBytes();
		for (int i = 0; i < originalBytes.length; i++) {
			assertEquals(originalBytes[i], restoredBytes[i]);
		}
	}

	@Test
	public void allChildrenWithFilterBase() {
		Exp filter1 = Exp.gt(Exp.mapLoopVar(LoopVarPart.VALUE), Exp.val(10));
		CTX[] ctxOne = new CTX[] {
			CTX.allChildrenWithFilter(filter1)
		};

		String base64StringOne = CTX.toBase64(ctxOne);
		CTX[] restoredContextOne = CTX.fromBase64(base64StringOne);

		assertEquals(ctxOne.length, restoredContextOne.length);
		assertEquals(ctxOne[0].id, restoredContextOne[0].id);
		Expression expression = ctxOne[0].exp;
		Expression restoredExpression = restoredContextOne[0].exp;
		assertEquals(expression.getBytes().length, restoredExpression.getBytes().length);

		// Test 2: String key filter
		Exp filterTwo = Exp.eq(Exp.mapLoopVar(LoopVarPart.MAP_KEY), Exp.val("target_key"));
		CTX[] contextTwo = new CTX[] {
			CTX.allChildrenWithFilter(filterTwo)
		};

		String base64StringTwo = CTX.toBase64(contextTwo);
		CTX[] restoredContextTwo = CTX.fromBase64(base64StringTwo);

		assertEquals(contextTwo.length, restoredContextTwo.length);
		assertEquals(contextTwo[0].id, restoredContextTwo[0].id);

		Expression expressionTwo = contextTwo[0].exp;
		Expression restoredExpressionTwo = restoredContextTwo[0].exp;
		assertEquals(expressionTwo.getBytes().length, restoredExpressionTwo.getBytes().length);

		// Test 3: Complex filter with AND/OR operations
		Exp filterThree = Exp.and(
			Exp.gt(Exp.mapLoopVar(LoopVarPart.VALUE), Exp.val(5)),
			Exp.lt(Exp.mapLoopVar(LoopVarPart.VALUE), Exp.val(50))
		);
		CTX[] contextThree = new CTX[] {
			CTX.allChildrenWithFilter(filterThree)
		};

		String base64StringThree = CTX.toBase64(contextThree);
		CTX[] restoredContextThree = CTX.fromBase64(base64StringThree);

		assertEquals(contextThree.length, restoredContextThree.length);
		assertEquals(contextThree[0].id, restoredContextThree[0].id);

		Expression expressionThree = contextThree[0].exp;
		Expression restoredExpressionThree = restoredContextThree[0].exp;

		assertEquals(expressionThree.getBytes().length, restoredExpressionThree.getBytes().length);

		// Test 4: Complex nested CTX with allChildrenWithFilter
		Exp filterFour = Exp.or(
			Exp.eq(Exp.mapLoopVar(LoopVarPart.MAP_KEY), Exp.val("key1")),
			Exp.eq(Exp.mapLoopVar(LoopVarPart.MAP_KEY), Exp.val("key2"))
		);
		CTX[] contextFour = new CTX[] {
			CTX.mapKey(Value.get("parent")),
			CTX.allChildrenWithFilter(filterFour)
		};

		String base64StringFour = CTX.toBase64(contextFour);
		CTX[] restoredContextFour = CTX.fromBase64(base64StringFour);

		assertEquals(contextFour.length, restoredContextFour.length);
		
		for (int i = 0; i < contextFour.length; i++) {
			assertEquals(contextFour[i].id, restoredContextFour[i].id);
			
			if (contextFour[i].id == 0x04) { // Exp.CTX_EXP - expression-based CTX
				Expression originalExpr = contextFour[i].exp;
				Expression restoredExpr = restoredContextFour[i].exp;
				
				assertEquals(originalExpr.getBytes().length, restoredExpr.getBytes().length);
				
				byte[] originalBytes = originalExpr.getBytes();
				byte[] restoredBytes = restoredExpr.getBytes();
				for (int j = 0; j < originalBytes.length; j++) {
					assertEquals(originalBytes[j], restoredBytes[j]);
				}
			} else if (contextFour[i].value != null) {
				Object objectOne = contextFour[i].value.getObject();
				Object objectTwo = restoredContextFour[i].value.getObject();
				assertEquals(objectOne, objectTwo);
			}
		}
	}

	@Test
	public void mixedContextWithAllChildrenBase() {
		CTX[] ctx1 = new CTX[] {
			CTX.mapKey(Value.get("root")),
			CTX.allChildren(),
			CTX.listIndex(-1),
			CTX.allChildrenWithFilter(Exp.gt(Exp.mapLoopVar(LoopVarPart.VALUE), Exp.val(0))),
			CTX.mapValue(Value.get("test"))
		};

		String base64 = CTX.toBase64(ctx1);
		CTX[] ctx2 = CTX.fromBase64(base64);

		assertEquals(ctx1.length, ctx2.length);

		for (int i = 0; i < ctx1.length; i++) {
			CTX itemOne = ctx1[i];
			CTX itemTwo = ctx2[i];

			assertEquals(itemOne.id, itemTwo.id);

			if (itemOne.id == 0x04) { // Exp.CTX_EXP - expression-based CTX
				Expression originalExpr = itemOne.exp;
				Expression restoredExpr = itemTwo.exp;
				
				assertEquals(originalExpr.getBytes().length, restoredExpr.getBytes().length);
				
				byte[] originalBytes = originalExpr.getBytes();
				byte[] restoredBytes = restoredExpr.getBytes();
				for (int j = 0; j < originalBytes.length; j++) {
					assertEquals(originalBytes[j], restoredBytes[j]);
				}
			} else if (itemOne.value != null) {
				Object objectOne = itemOne.value.getObject();
				Object objectTwo = itemTwo.value.getObject();

				if (objectOne instanceof Integer && objectTwo instanceof Long) {
					assertEquals((long)(Integer)objectOne, (long)(Long)objectTwo);
				}
				else {
					assertEquals(objectOne, objectTwo);
				}
			}
		}
	}
}
