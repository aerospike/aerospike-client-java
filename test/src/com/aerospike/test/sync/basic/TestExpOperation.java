/*
 * Copyright 2012-2021 Aerospike, Inc.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value.HLLValue;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.HLLExp;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.test.sync.TestSync;

public class TestExpOperation extends TestSync {
	String binA = "A";
	String binB = "B";
	String binC = "C";
	String binD = "D";
	String binH = "H";
	String expVar = "EV";

	Key keyA = new Key(args.namespace, args.set, "A");
	Key keyB = new Key(args.namespace, args.set, new byte[] {(byte)'B'});

	@Before
	public void setUp() throws Exception {
		client.delete(null, keyA);
		client.delete(null, keyB);

		client.put(null, keyA, new Bin(binA, 1), new Bin(binD, 2));
		client.put(null, keyB, new Bin(binB, 2), new Bin(binD, 2));
	}

	@Test
	public void expReadEvalError() {
		Expression exp = Exp.build(Exp.add(Exp.intBin(binA), Exp.val(4)));

		Record record = client.operate(null, keyA, ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT));
		assertRecordFound(keyA, record);
		//System.out.println(record);

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				// Bin AString doesn't exist on keyB.
				client.operate(null, keyB, ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT));
			}
		});

		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());

		// Try NO_FAIL.
		record = client.operate(null, keyB, ExpOperation.read(expVar, exp, ExpReadFlags.EVAL_NO_FAIL));
		assertRecordFound(keyB, record);
		//System.out.println(record);
	}

	@Test
	public void expReadOnWriteEvalError() {
		Expression wexp = Exp.build(Exp.intBin(binD));
		Expression rexp = Exp.build(Exp.intBin(binA));

		Record record = client.operate(null, keyA,
			ExpOperation.write(binD, wexp, ExpWriteFlags.DEFAULT),
			ExpOperation.read(expVar, rexp, ExpReadFlags.DEFAULT)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.operate(null, keyB,
					ExpOperation.write(binD, wexp, ExpWriteFlags.DEFAULT),
					ExpOperation.read(expVar, rexp, ExpReadFlags.DEFAULT)
					);
			}
		});

		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());

		// Add NO_FAIL.
		record = client.operate(null, keyB, ExpOperation.read(expVar, rexp, ExpReadFlags.EVAL_NO_FAIL));
		assertRecordFound(keyB, record);
		//System.out.println(record);
	}

	@Test
	public void expWriteEvalError() {
		Expression wexp = Exp.build(Exp.add(Exp.intBin(binA), Exp.val(4)));
		Expression rexp = Exp.build(Exp.intBin(binC));

		Record record = client.operate(null, keyA,
			ExpOperation.write(binC, wexp, ExpReadFlags.DEFAULT),
			ExpOperation.read(expVar, rexp, ExpReadFlags.DEFAULT)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.operate(null, keyB,
					ExpOperation.write(binC, wexp, ExpWriteFlags.DEFAULT),
					ExpOperation.read(expVar, rexp, ExpReadFlags.DEFAULT)
					);
			}
		});

		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());

		// Add NO_FAIL.
		record = client.operate(null, keyB,
			ExpOperation.write(binC, wexp, ExpWriteFlags.EVAL_NO_FAIL),
			ExpOperation.read(expVar, rexp, ExpReadFlags.EVAL_NO_FAIL)
			);
		assertRecordFound(keyB, record);
		//System.out.println(record);
	}

	@Test
	public void expWritePolicyError() {
		Expression wexp = Exp.build(Exp.add(Exp.intBin(binA), Exp.val(4)));

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.operate(null, keyA,
					ExpOperation.write(binC, wexp, ExpWriteFlags.UPDATE_ONLY)
					);
			}
		});

		assertEquals(ResultCode.BIN_NOT_FOUND, ae.getResultCode());

		Record record = client.operate(null, keyA,
			ExpOperation.write(binC, wexp, ExpWriteFlags.UPDATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL)
			);
		assertRecordFound(keyA, record);
		//System.out.println(record);

		record = client.operate(null, keyA,
			ExpOperation.write(binC, wexp, ExpWriteFlags.CREATE_ONLY)
			);
		assertRecordFound(keyA, record);
		//System.out.println(record);

		ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.operate(null, keyA,
					ExpOperation.write(binC, wexp, ExpWriteFlags.CREATE_ONLY)
					);
			}
		});

		assertEquals(ResultCode.BIN_EXISTS_ERROR, ae.getResultCode());

		record = client.operate(null, keyA,
			ExpOperation.write(binC, wexp, ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL)
			);
		assertRecordFound(keyA, record);

		Expression dexp = Exp.build(Exp.nil());

		ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.operate(null, keyA,
					ExpOperation.write(binC, dexp, ExpWriteFlags.DEFAULT)
					);
			}
		});

		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());

		record = client.operate(null, keyA,
			ExpOperation.write(binC, dexp, ExpWriteFlags.POLICY_NO_FAIL)
			);
		assertRecordFound(keyA, record);

		record = client.operate(null, keyA,
			ExpOperation.write(binC, dexp, ExpWriteFlags.ALLOW_DELETE)
			);
		assertRecordFound(keyA, record);

		record = client.operate(null, keyA,
			ExpOperation.write(binC, wexp, ExpWriteFlags.CREATE_ONLY)
			);
		assertRecordFound(keyA, record);
	}

	@Test
	public void expReturnsUnknown() {
		Expression exp = Exp.build(
			Exp.cond(
				Exp.eq(Exp.intBin(binC), Exp.val(5)), Exp.unknown(),
				Exp.binExists(binA), Exp.val(5),
				Exp.unknown()));

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.operate(null, keyA,
					ExpOperation.write(binC, exp, ExpWriteFlags.DEFAULT),
					Operation.get(binC)
					);			}
		});

		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());

		Record record = client.operate(null, keyA,
			ExpOperation.write(binC, exp, ExpWriteFlags.EVAL_NO_FAIL),
			Operation.get(binC)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		List<?> results = record.getList(binC);
		Object val = results.get(0);
		assertEquals(null, val);
		val = results.get(1);
		assertEquals(null, val);
	}

	@Test
	public void expReturnsNil() {
		Expression exp = Exp.build(Exp.nil());

		Record record = client.operate(null, keyA,
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT),
			Operation.get(binC)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		Object val = record.getValue(expVar);
		assertEquals(null, val);
	}

	@Test
	public void expReturnsInt() {
		Expression exp = Exp.build(Exp.add(Exp.intBin(binA), Exp.val(4)));

		Record record = client.operate(null, keyA,
			ExpOperation.write(binC, exp, ExpWriteFlags.DEFAULT),
			Operation.get(binC),
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		List<?> results = record.getList(binC);
		long val = (Long)results.get(1);
		assertEquals(5, val);

		val = record.getLong(expVar);
		assertEquals(5, val);

		record = client.operate(null, keyA,
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		val = record.getLong(expVar);
		assertEquals(5, val);
	}

	@Test
	public void expReturnsFloat() {
		Expression exp = Exp.build(Exp.add(Exp.toFloat(Exp.intBin(binA)), Exp.val(4.0)));

		Record record = client.operate(null, keyA,
			ExpOperation.write(binC, exp, ExpWriteFlags.DEFAULT),
			Operation.get(binC),
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		List<?> results = record.getList(binC);
		double val = (Double)results.get(1);
		double delta = 0.000001;
		assertEquals(5.0, val, delta);

		val = record.getDouble(expVar);
		assertEquals(5.0, val, delta);

		record = client.operate(null, keyA,
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		val = record.getDouble(expVar);
		assertEquals(5.0, val, delta);
	}

	@Test
	public void expReturnsString() {
		String str = "xxx";
		Expression exp = Exp.build(Exp.val(str));

		Record record = client.operate(null, keyA,
			ExpOperation.write(binC, exp, ExpWriteFlags.DEFAULT),
			Operation.get(binC),
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		List<?> results = record.getList(binC);
		String val = (String)results.get(1);
		assertEquals(str, val);

		val = record.getString(expVar);
		assertEquals(str, val);

		record = client.operate(null, keyA,
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		val = record.getString(expVar);
		assertEquals(str, val);
	}

	@Test
	public void expReturnsBlob() {
		byte[] bytes = new byte[] {0x78, 0x78, 0x78};
		Expression exp = Exp.build(Exp.val(bytes));

		Record record = client.operate(null, keyA,
			ExpOperation.write(binC, exp, ExpWriteFlags.DEFAULT),
			Operation.get(binC),
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		List<?> results = record.getList(binC);
		byte[] val = (byte[])results.get(1);
		String resultString = "bytes not equal";
		assertArrayEquals(resultString, bytes, val);

		val = (byte[])record.getValue(expVar);
		assertArrayEquals(resultString, bytes, val);

		record = client.operate(null, keyA,
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		val = (byte[])record.getValue(expVar);
		assertArrayEquals(resultString, bytes, val);
	}

	@Test
	public void expReturnsBoolean() {
		Expression exp = Exp.build(
			Exp.eq(Exp.intBin(binA), Exp.val(1)));

		Record record = client.operate(null, keyA,
			ExpOperation.write(binC, exp, ExpWriteFlags.DEFAULT),
			Operation.get(binC),
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		List<?> results = record.getList(binC);
		boolean val = (Boolean)results.get(1);
		assertTrue(val);

		val = record.getBoolean(expVar);
		assertTrue(val);
	}

	@Test
	public void expReturnsHLL() {
		Expression exp = Exp.build(HLLExp.init(HLLPolicy.Default, Exp.val(4), Exp.nil()));

		Record record = client.operate(null, keyA,
			HLLOperation.init(HLLPolicy.Default, binH, 4),
			ExpOperation.write(binC, exp, ExpWriteFlags.DEFAULT),
			Operation.get(binH),
			Operation.get(binC),
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		List<?> results = record.getList(binH);
		HLLValue valH = (HLLValue)results.get(1);

		results = record.getList(binC);
		HLLValue valC = (HLLValue)results.get(1);

		HLLValue valExp = record.getHLLValue(expVar);

		String resultString = "bytes not equal";
		assertArrayEquals(resultString, valH.getBytes(), valC.getBytes());
		assertArrayEquals(resultString, valH.getBytes(), valExp.getBytes());

		record = client.operate(null, keyA,
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		valExp = record.getHLLValue(expVar);
		assertArrayEquals(resultString, valH.getBytes(), valExp.getBytes());
	}
}
