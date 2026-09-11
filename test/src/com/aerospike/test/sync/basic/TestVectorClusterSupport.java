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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.VectorExp;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Packer;
import com.aerospike.client.vector.Vector;
import com.aerospike.client.vector.VectorDistanceMetric;
import com.aerospike.test.util.TestBase;

/**
 * Vector cluster support tests.
 */
public class TestVectorClusterSupport extends TestBase {
	private static final Key key = new Key("test", "set", "veckey");
	private static final Vector vec = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});

	private static final String EXPECTED_MESSAGE = "Vector is not supported by all nodes in the cluster";

	private static Command newCommand(boolean vectorSupported) {
		Command cmd = new Command(0, 0, 0) {
			@Override
			protected void sizeBuffer() {
				if (dataBuffer == null || dataBuffer.length < dataOffset) {
					dataBuffer = new byte[Math.max(8192, dataOffset)];
				}
			}
		};
		cmd.vectorSupported = vectorSupported;
		return cmd;
	}

	private static Expression vectorFilter() {
		return Exp.build(
			Exp.gt(
				VectorExp.distance(VectorDistanceMetric.COSINE, vec, Exp.vectorBin("v")),
				Exp.val(0.5)));
	}

	//-------------------------------------------------------
	// Value.hasVector detection
	//-------------------------------------------------------

	@Test
	public void valueVectorHasVector() {
		assertTrue(Value.get(vec).hasVector());
	}

	@Test
	public void valueScalarHasNoVector() {
		assertFalse(Value.get(123L).hasVector());
		assertFalse(Value.get("abc").hasVector());
	}

	@Test
	public void valueListNestedVector() {
		assertTrue(Value.get((Object)Collections.singletonList(vec)).hasVector());
		assertFalse(Value.get((Object)Collections.singletonList("abc")).hasVector());
	}

	@Test
	public void valueMapNestedVector() {
		assertTrue(Value.get((Object)Collections.singletonMap("k", vec)).hasVector());
		assertFalse(Value.get((Object)Collections.singletonMap("k", "v")).hasVector());
	}

	@Test
	public void valueDetectsAddedVector() {
		List<Object> list = new ArrayList<>();
		Value value = Value.get((Object)list);

		assertFalse(value.hasVector());
		list.add(vec);
		assertTrue(value.hasVector());
		list.clear();
		assertTrue(value.hasVector());
	}

	//-------------------------------------------------------
	// Packer.hasVector detection
	//-------------------------------------------------------

	@Test
	public void packerTracksVector() {
		Packer withVector = new Packer();
		withVector.packVector(vec);
		assertTrue(withVector.hasVector());

		Packer withoutVector = new Packer();
		withoutVector.packInt(1);
		assertFalse(withoutVector.hasVector());
	}

	//-------------------------------------------------------
	// Expression.hasVector detection
	//-------------------------------------------------------

	@Test
	public void expressionWithVector() {
		assertTrue(vectorFilter().hasVector());
	}

	@Test
	public void expressionWithoutVector() {
		assertFalse(Exp.build(Exp.eq(Exp.intBin("a"), Exp.val(1))).hasVector());
	}

	@Test
	public void expressionFromBytesReportsNoVector() {
		assertFalse(Expression.fromBytes(new byte[] {1, 2, 3}).hasVector());
	}

	//-------------------------------------------------------
	// Operation vector detection
	//-------------------------------------------------------

	@Test
	public void operationBinVector() {
		assertTrue(Operation.put(new Bin("v", vec)).value.hasVector());
		assertFalse(Operation.put(new Bin("v", 1)).value.hasVector());
		assertFalse(Operation.get("v").value.hasVector());
	}

	@Test
	public void listOperationVector() {
		assertTrue(ListOperation.append("v", Value.get(vec)).value.hasVector());
		assertFalse(ListOperation.append("v", Value.get(1)).value.hasVector());
		assertTrue(ListOperation.appendItems("v", Collections.singletonList(Value.get(vec))).value.hasVector());
		assertTrue(ListOperation.insertItems("v", 0, Collections.singletonList(Value.get(vec))).value.hasVector());
	}

	@Test
	public void mapOperationVector() {
		assertTrue(MapOperation.put(MapPolicy.Default, "m", Value.get("k"), Value.get(vec)).value.hasVector());
		assertFalse(MapOperation.put(MapPolicy.Default, "m", Value.get("k"), Value.get(1)).value.hasVector());

		Map<Value,Value> items = new HashMap<>();
		items.put(Value.get("k"), Value.get(vec));
		assertTrue(MapOperation.putItems(MapPolicy.Default, "m", items).value.hasVector());
	}

	@Test
	public void hllOperationVector() {
		assertTrue(HLLOperation.add(HLLPolicy.Default, "h", Collections.singletonList(Value.get(vec))).value.hasVector());
	}

	@Test
	public void expOperationVector() {
		assertTrue(ExpOperation.write("v", vectorFilter(), 0).value.hasVector());
		assertFalse(ExpOperation.write("v", Exp.build(Exp.val(1)), 0).value.hasVector());
	}

	//-------------------------------------------------------
	// Command.checkVectorSupport guard
	//-------------------------------------------------------

	@Test
	public void checkVectorSupportThrowsWhenUnsupported() {
		try {
			newCommand(false).checkVectorSupport(true);
			fail("Expected AerospikeException");
		}
		catch (AerospikeException e) {
			assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode());
			assertTrue(e.getMessage().contains(EXPECTED_MESSAGE));
		}
	}

	@Test
	public void checkVectorSupportAllowsWhenSupported() {
		newCommand(false).checkVectorSupport(false);
		newCommand(true).checkVectorSupport(true);
	}

	//-------------------------------------------------------
	// End-to-end serialization: write (bin) path
	//-------------------------------------------------------

	@Test
	public void writeVectorBinFailsWhenUnsupported() {
		assertWriteThrows(new Bin("v", vec));
	}

	@Test
	public void writeVectorInListBinFailsWhenUnsupported() {
		assertWriteThrows(new Bin("v", Collections.singletonList(vec)));
	}

	@Test
	public void writeVectorInMapBinFailsWhenUnsupported() {
		assertWriteThrows(new Bin("v", Collections.singletonMap("k", vec)));
	}

	@Test
	public void writeScalarBinSucceedsWhenUnsupported() {
		newCommand(false).setWrite(new WritePolicy(), Operation.Type.WRITE, key, new Bin[] {new Bin("s", 1)});
	}

	@Test
	public void writeVectorBinSucceedsWhenSupported() {
		newCommand(true).setWrite(new WritePolicy(), Operation.Type.WRITE, key, new Bin[] {new Bin("v", vec)});
	}

	private void assertWriteThrows(Bin bin) {
		try {
			newCommand(false).setWrite(new WritePolicy(), Operation.Type.WRITE, key, new Bin[] {bin});
			fail("Expected AerospikeException");
		}
		catch (AerospikeException e) {
			assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode());
			assertTrue(e.getMessage().contains(EXPECTED_MESSAGE));
		}
	}

	//-------------------------------------------------------
	// End-to-end serialization: filter expression path
	//-------------------------------------------------------

	@Test
	public void filterExpVectorFailsWhenUnsupported() {
		WritePolicy wp = new WritePolicy();
		wp.filterExp = vectorFilter();

		try {
			newCommand(false).setWrite(wp, Operation.Type.WRITE, key, new Bin[] {new Bin("s", 1)});
			fail("Expected AerospikeException");
		}
		catch (AerospikeException e) {
			assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode());
			assertTrue(e.getMessage().contains(EXPECTED_MESSAGE));
		}
	}

	//-------------------------------------------------------
	// End-to-end serialization: operate (CDT) path
	//-------------------------------------------------------

	@Test
	public void operateVectorCdtFailsWhenUnsupported() {
		Operation[] ops = new Operation[] {ListOperation.append("v", Value.get(vec))};
		WritePolicy wp = new WritePolicy();
		OperateArgs args = new OperateArgs(wp, null, null, ops);

		try {
			newCommand(false).setOperate(args.writePolicy, key, args);
			fail("Expected AerospikeException");
		}
		catch (AerospikeException e) {
			assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode());
			assertTrue(e.getMessage().contains(EXPECTED_MESSAGE));
		}
	}

	@Test
	public void operateVectorPutFailsWhenUnsupported() {
		Operation[] ops = new Operation[] {Operation.put(new Bin("v", vec))};
		WritePolicy wp = new WritePolicy();
		OperateArgs args = new OperateArgs(wp, null, null, ops);

		try {
			newCommand(false).setOperate(args.writePolicy, key, args);
			fail("Expected AerospikeException");
		}
		catch (AerospikeException e) {
			assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode());
			assertTrue(e.getMessage().contains(EXPECTED_MESSAGE));
		}
	}
}
