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

package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchDelete;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.test.sync.TestSync;

public class TestFilterExp extends TestSync {
	String binA = "A";
	String binB = "B";
	String binC = "C";
	String binD = "D";
	String binE = "E";

	Key keyA = new Key(args.namespace, args.set, "A");
	Key keyB = new Key(args.namespace, args.set, new byte[] {(byte)'B'});
	Key keyC = new Key(args.namespace, args.set, "C");

	@BeforeClass
	public static void register() {
		RegisterTask task = client.register(null,
				TestUDF.class.getClassLoader(), "udf/record_example.lua",
				"record_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	@Before
	public void setUp() throws Exception {
		client.delete(null, keyA);
		client.delete(null, keyB);
		client.delete(null, keyC);

		client.put(null, keyA, new Bin(binA, 1), new Bin(binB, 1.1), new Bin(binC, "abcde"), new Bin(binD, 1), new Bin(binE, -1));
		client.put(null, keyB, new Bin(binA, 2), new Bin(binB, 2.2), new Bin(binC, "abcdeabcde"), new Bin(binD, 1), new Bin(binE, -2));
		client.put(null, keyC, new Bin(binA, 0), new Bin(binB, -1), new Bin(binC, 1));
	}

	@Test
	public void put() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		Bin bin = new Bin(binA, 3);

		client.put(policy, keyA, bin);
		Record r = client.get(null, keyA);

		assertBinEqual(keyA, r, binA, 3);

		client.put(policy, keyB, bin);
		r = client.get(null, keyB);

		assertBinEqual(keyB, r, binA, 2);
	}

	@Test
	public void putExcept() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		policy.failOnFilteredOut = true;

		Bin bin = new Bin(binA, 3);

		client.put(policy, keyA, bin);

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.put(policy, keyB, bin);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void get() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		Record r = client.get(policy, keyA);

		assertBinEqual(keyA, r, binA, 1);

		r = client.get(policy, keyB);

		assertEquals(null, r);
	}

	@Test
	public void getExcept() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		policy.failOnFilteredOut = true;

		client.get(policy, keyA);

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyB);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void batch() {
		BatchPolicy policy = new BatchPolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		Key[] keys = { keyA, keyB };

		Record[] records = client.get(policy, keys);

		assertBinEqual(keyA, records[0], binA, 1);
		assertEquals(null, records[1]);
	}

	@Test
	public void delete() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		client.delete(policy, keyA);
		Record r = client.get(null, keyA);

		assertEquals(null, r);

		client.delete(policy, keyB);
		r = client.get(null,  keyB);

		assertBinEqual(keyB, r, binA, 2);
	}

	@Test
	public void deleteExcept() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		policy.failOnFilteredOut = true;

		client.delete(policy, keyA);

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.delete(policy, keyB);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void durableDelete() {
		org.junit.Assume.assumeTrue(args.enterprise);

		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		policy.durableDelete = true;

		client.delete(policy, keyA);
		Record r = client.get(null, keyA);

		assertEquals(null, r);

		client.delete(policy, keyB);
		r = client.get(null,  keyB);

		assertBinEqual(keyB, r, binA, 2);
	}

	@Test
	public void durableDeleteExcept() {
		org.junit.Assume.assumeTrue(args.enterprise);

		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		policy.failOnFilteredOut = true;
		policy.durableDelete = true;

		client.delete(policy, keyA);

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.delete(policy, keyB);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void operateRead() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		Record r = client.operate(policy, keyA, Operation.get(binA));

		assertBinEqual(keyA, r, binA, 1);

		r = client.operate(policy, keyB, Operation.get(binA));

		assertEquals(null, r);
	}

	@Test
	public void operateReadExcept() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		policy.failOnFilteredOut = true;

		client.operate(policy, keyA, Operation.get(binA));

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.operate(policy, keyB, Operation.get(binA));
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void operateWrite() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		Bin bin = new Bin(binA, 3);

		Record r = client.operate(policy, keyA,
				Operation.put(bin), Operation.get(binA));

		assertBinEqual(keyA, r, binA, 3);

		r = client.operate(policy, keyB,
				Operation.put(bin), Operation.get(binA));

		assertEquals(null, r);
	}

	@Test
	public void operateWriteExcept() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		policy.failOnFilteredOut = true;

		Bin bin = new Bin(binA, 3);

		client.operate(policy, keyA,
				Operation.put(bin), Operation.get(binA));

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.operate(policy, keyB,
					Operation.put(bin), Operation.get(binA));
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void udf() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		client.execute(policy, keyA, "record_example", "writeBin",
				Value.get(binA), Value.get(3));

		Record r = client.get(null, keyA);

		assertBinEqual(keyA, r, binA, 3);

		client.execute(policy, keyB, "record_example", "writeBin",
				Value.get(binA), Value.get(3));

		r = client.get(null, keyB);

		assertBinEqual(keyB, r, binA, 2);
	}

	@Test
	public void udfExcept() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		policy.failOnFilteredOut = true;

		client.execute(policy, keyA, "record_example", "writeBin",
				Value.get(binA), Value.get(3));

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.execute(policy, keyB, "record_example", "writeBin",
					Value.get(binA), Value.get(3));
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void filterExclusive() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.exclusive(
				Exp.eq(Exp.intBin(binA), Exp.val(1)),
				Exp.eq(Exp.intBin(binD), Exp.val(1))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterAddInt() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.add(Exp.intBin(binA), Exp.intBin(binD), Exp.val(1)),
				Exp.val(4)));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterAddFloat() {
		String name = "val";

		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.let(
				Exp.def(name, Exp.add(Exp.floatBin(binB), Exp.val(1.1))),
				Exp.and(
					Exp.ge(Exp.var(name), Exp.val(3.2999)),
					Exp.le(Exp.var(name), Exp.val(3.3001)))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterSub() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.sub(Exp.val(1), Exp.intBin(binA), Exp.intBin(binD)),
				Exp.val(-2)));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterMul() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.mul(Exp.val(2), Exp.intBin(binA), Exp.intBin(binD)),
				Exp.val(4)));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterDiv() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.div(Exp.val(8), Exp.intBin(binA), Exp.intBin(binD)),
				Exp.val(4)));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterPow() {
		String name = "x";

		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.let(
				Exp.def(name, Exp.pow(Exp.floatBin(binB), Exp.val(2.0))),
				Exp.and(
					Exp.ge(Exp.var(name), Exp.val(4.8399)),
					Exp.le(Exp.var(name), Exp.val(4.8401)))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterLog() {
		String name = "x";

		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.let(
				Exp.def(name, Exp.log(Exp.floatBin(binB), Exp.val(2.0))),
				Exp.and(
					Exp.ge(Exp.var(name), Exp.val(1.1374)),
					Exp.le(Exp.var(name), Exp.val(1.1376)))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterMod() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.mod(Exp.intBin(binA), Exp.val(2)),
				Exp.val(0)));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterAbs() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.abs(Exp.intBin(binE)),
				Exp.val(2)));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterFloor() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.floor(Exp.floatBin(binB)),
				Exp.val(2.0)));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterCeil() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.ceil(Exp.floatBin(binB)),
				Exp.val(3.0)));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterToInt() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.toInt(Exp.floatBin(binB)),
				Exp.val(2)));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterToFloat() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.toFloat(Exp.intBin(binA)),
				Exp.val(2.0)));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		Record r = client.get(policy, keyB);
		assertBinEqual(keyA, r, binA, 2);
	}

	@Test
	public void filterIntAnd() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.and(
					Exp.eq(
						Exp.intAnd(Exp.intBin(binA), Exp.val(0)),
						Exp.val(0)),
					Exp.eq(
						Exp.intAnd(Exp.intBin(binA), Exp.val(0xFFFF)),
						Exp.val(1)))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.and(
				Exp.eq(
					Exp.intAnd(Exp.intBin(binA), Exp.val(0)),
					Exp.val(0)),
				Exp.eq(
					Exp.intAnd(Exp.intBin(binA), Exp.val(0xFFFF)),
					Exp.val(1))));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void filterIntOr() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.and(
					Exp.eq(
						Exp.intOr(Exp.intBin(binA), Exp.val(0)),
						Exp.val(1)),
					Exp.eq(
						Exp.intOr(Exp.intBin(binA), Exp.val(0xFF)),
						Exp.val(0xFF)))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.and(
				Exp.eq(
					Exp.intOr(Exp.intBin(binA), Exp.val(0)),
					Exp.val(1)),
				Exp.eq(
					Exp.intOr(Exp.intBin(binA), Exp.val(0xFF)),
					Exp.val(0xFF))));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void filterIntXor() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.and(
					Exp.eq(
						Exp.intXor(Exp.intBin(binA), Exp.val(0)),
						Exp.val(1)),
					Exp.eq(
						Exp.intXor(Exp.intBin(binA), Exp.val(0xFF)),
						Exp.val(0xFE)))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.and(
				Exp.eq(
					Exp.intXor(Exp.intBin(binA), Exp.val(0)),
					Exp.val(1)),
				Exp.eq(
					Exp.intXor(Exp.intBin(binA), Exp.val(0xFF)),
					Exp.val(0xFE))));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void filterIntNot() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.eq(
					Exp.intNot(Exp.intBin(binA)),
					Exp.val(-2))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.intNot(Exp.intBin(binA)),
				Exp.val(-2)));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void filterLshift() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.eq(
					Exp.lshift(Exp.intBin(binA), Exp.val(2)),
					Exp.val(4))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.lshift(Exp.intBin(binA), Exp.val(2)),
				Exp.val(4)));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void filterRshift() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.eq(
					Exp.rshift(Exp.intBin(binE), Exp.val(62)),
					Exp.val(3))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyB);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.rshift(Exp.intBin(binE), Exp.val(62)),
				Exp.val(3)));

		Record r = client.get(policy, keyB);
		assertBinEqual(keyB, r, binE, -2);
	}

	@Test
	public void filterARshift() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.eq(
					Exp.arshift(Exp.intBin(binE), Exp.val(62)),
					Exp.val(-1))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyB);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.arshift(Exp.intBin(binE), Exp.val(62)),
				Exp.val(-1)));

		Record r = client.get(policy, keyB);
		assertBinEqual(keyB, r, binE, -2);
	}

	@Test
	public void filterBitCount() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.eq(
					Exp.count(Exp.intBin(binA)),
					Exp.val(1))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.count(Exp.intBin(binA)),
				Exp.val(1)));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void filterLscan() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.eq(
					Exp.lscan(Exp.intBin(binA), Exp.val(true)),
					Exp.val(63))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.lscan(Exp.intBin(binA), Exp.val(true)),
				Exp.val(63)));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void filterRscan() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.eq(
					Exp.rscan(Exp.intBin(binA), Exp.val(true)),
					Exp.val(63))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.rscan(Exp.intBin(binA), Exp.val(true)),
				Exp.val(63)));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void filterMin() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.eq(
					Exp.min(Exp.intBin(binA), Exp.intBin(binD), Exp.intBin(binE)),
					Exp.val(-1))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.min(Exp.intBin(binA), Exp.intBin(binD), Exp.intBin(binE)),
				Exp.val(-1)));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void filterMax() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.eq(
					Exp.max(Exp.intBin(binA), Exp.intBin(binD), Exp.intBin(binE)),
					Exp.val(1))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.max(Exp.intBin(binA), Exp.intBin(binD), Exp.intBin(binE)),
				Exp.val(1)));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void filterCond() {
		Policy policy = new Policy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.eq(
					Exp.cond(
						Exp.eq(Exp.intBin(binA), Exp.val(0)), Exp.add(Exp.intBin(binD), Exp.intBin(binE)),
						Exp.eq(Exp.intBin(binA), Exp.val(1)), Exp.sub(Exp.intBin(binD), Exp.intBin(binE)),
						Exp.eq(Exp.intBin(binA), Exp.val(2)), Exp.mul(Exp.intBin(binD), Exp.intBin(binE)),
						Exp.val(-1)),
					Exp.val(2))));
		policy.failOnFilteredOut = true;

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.get(policy, keyA);
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		policy.filterExp = Exp.build(
			Exp.eq(
				Exp.cond(
					Exp.eq(Exp.intBin(binA), Exp.val(0)), Exp.add(Exp.intBin(binD), Exp.intBin(binE)),
					Exp.eq(Exp.intBin(binA), Exp.val(1)), Exp.sub(Exp.intBin(binD), Exp.intBin(binE)),
					Exp.eq(Exp.intBin(binA), Exp.val(2)), Exp.mul(Exp.intBin(binD), Exp.intBin(binE)),
					Exp.val(-1)),
				Exp.val(2)));

		Record r = client.get(policy, keyA);
		assertBinEqual(keyA, r, binA, 1);
	}

	@Test
	public void batchKeyFilter() {
		// Write/Delete records with filter.
		BatchWritePolicy wp = new BatchWritePolicy();
		wp.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		BatchDeletePolicy dp = new BatchDeletePolicy();
		dp.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(0)));

		Operation[] put = Operation.array(Operation.put(new Bin(binA, 3)));

		List<BatchRecord> brecs = new ArrayList<BatchRecord>();
		brecs.add(new BatchWrite(wp, keyA, put));
		brecs.add(new BatchWrite(wp, keyB, put));
		brecs.add(new BatchDelete(dp, keyC));

		boolean status = client.operate(null, brecs);
		assertFalse(status); // Filtered out result code causes status to be false.

		BatchRecord br = brecs.get(0);
		assertEquals(ResultCode.OK, br.resultCode);

		br = brecs.get(1);
		assertEquals(ResultCode.FILTERED_OUT, br.resultCode);

		br = brecs.get(2);
		assertEquals(ResultCode.OK, br.resultCode);

		// Read records
		Key[] keys = new Key[] {keyA, keyB, keyC};
		Record[] recs = client.get(null, keys, binA);

		Record r = recs[0];
		assertBinEqual(keyA, r, binA, 3);

		r = recs[1];
		assertBinEqual(keyB, r, binA, 2);

		r = recs[2];
		assertNull(r);
	}
}
