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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.Value.HLLValue;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.HLLExp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.test.sync.TestSync;

public class TestHLLExp extends TestSync {
	String bin1 = "hllbin_1";
	String bin2 = "hllbin_2";
	String bin3 = "hllbin_3";
	Policy policy = new Policy();
	HLLValue hll1;
	HLLValue hll2;
	HLLValue hll3;

	@Test
	public void hllExp() {
		Key key = new Key(args.namespace, args.set, 5200);
		client.delete(null, key);

		ArrayList<Value> list1 = new ArrayList<Value>();
		list1.add(Value.get("Akey1"));
		list1.add(Value.get("Akey2"));
		list1.add(Value.get("Akey3"));

		ArrayList<Value> list2 = new ArrayList<Value>();
		list2.add(Value.get("Bkey1"));
		list2.add(Value.get("Bkey2"));
		list2.add(Value.get("Bkey3"));

		ArrayList<Value> list3 = new ArrayList<Value>();
		list3.add(Value.get("Akey1"));
		list3.add(Value.get("Akey2"));
		list3.add(Value.get("Bkey1"));
		list3.add(Value.get("Bkey2"));
		list3.add(Value.get("Ckey1"));
		list3.add(Value.get("Ckey2"));

		Record rec = client.operate(null, key,
			HLLOperation.add(HLLPolicy.Default, bin1, list1, 8),
			HLLOperation.add(HLLPolicy.Default, bin2, list2, 8),
			HLLOperation.add(HLLPolicy.Default, bin3, list3, 8),
			Operation.get(bin1),
			Operation.get(bin2),
			Operation.get(bin3)
			);

		List<?> results = rec.getList(bin1);
		hll1 = (HLLValue)results.get(1);
		assertNotEquals(null, hll1);

		results = rec.getList(bin2);
		hll2 = (HLLValue)results.get(1);
		assertNotEquals(null, hll2);

		results = rec.getList(bin3);
		hll3 = (HLLValue)results.get(1);
		assertNotEquals(null, hll3);

		count(key);
		union(key);
		intersect(key);
		similarity(key);
		describe(key);
		mayContain(key);
		add(key);
	}

	private void count(Key key) {
		policy.filterExp = Exp.build(Exp.eq(HLLExp.getCount(Exp.hllBin(bin1)), Exp.val(0)));
		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(Exp.gt(HLLExp.getCount(Exp.hllBin(bin1)), Exp.val(0)));
		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void union(Key key) {
		ArrayList<HLLValue> hlls = new ArrayList<HLLValue>();
		hlls.add(hll1);
		hlls.add(hll2);
		hlls.add(hll3);

		policy.filterExp = Exp.build(
			Exp.ne(
				HLLExp.getCount(HLLExp.getUnion(Exp.val(hlls), Exp.hllBin(bin1))),
				HLLExp.getUnionCount(Exp.val(hlls), Exp.hllBin(bin1))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				HLLExp.getCount(HLLExp.getUnion(Exp.val(hlls), Exp.hllBin(bin1))),
				HLLExp.getUnionCount(Exp.val(hlls), Exp.hllBin(bin1))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void intersect(Key key) {
		ArrayList<HLLValue> hlls2 = new ArrayList<HLLValue>();
		hlls2.add(hll2);

		ArrayList<HLLValue> hlls3 = new ArrayList<HLLValue>();
		hlls3.add(hll3);

		policy.filterExp = Exp.build(
			Exp.ge(
				HLLExp.getIntersectCount(Exp.val(hlls2), Exp.hllBin(bin1)),
				HLLExp.getIntersectCount(Exp.val(hlls3), Exp.hllBin(bin1))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.le(
				HLLExp.getIntersectCount(Exp.val(hlls2), Exp.hllBin(bin1)),
				HLLExp.getIntersectCount(Exp.val(hlls3), Exp.hllBin(bin1))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void similarity(Key key) {
		ArrayList<HLLValue> hlls2 = new ArrayList<HLLValue>();
		hlls2.add(hll2);

		ArrayList<HLLValue> hlls3 = new ArrayList<HLLValue>();
		hlls3.add(hll3);

		policy.filterExp = Exp.build(
			Exp.ge(
				HLLExp.getSimilarity(Exp.val(hlls2), Exp.hllBin(bin1)),
				HLLExp.getSimilarity(Exp.val(hlls3), Exp.hllBin(bin1))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.le(
				HLLExp.getSimilarity(Exp.val(hlls2), Exp.hllBin(bin1)),
				HLLExp.getSimilarity(Exp.val(hlls3), Exp.hllBin(bin1))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void describe(Key key) {
		Exp index = Exp.val(0);

		policy.filterExp = Exp.build(
			Exp.ne(
				ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, index,
					HLLExp.describe(Exp.hllBin(bin1))),
				ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, index,
					HLLExp.describe(Exp.hllBin(bin2)))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, index,
					HLLExp.describe(Exp.hllBin(bin1))),
				ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, index,
					HLLExp.describe(Exp.hllBin(bin2)))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void mayContain(Key key) {
		ArrayList<Value> values = new ArrayList<Value>();
		values.add(Value.get("new_val"));

		policy.filterExp = Exp.build(
			Exp.eq(HLLExp.mayContain(Exp.val(values), Exp.hllBin(bin2)), Exp.val(1))
			);

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.ne(HLLExp.mayContain(Exp.val(values), Exp.hllBin(bin2)), Exp.val(1))
			);

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void add(Key key) {
		ArrayList<Value> values = new ArrayList<Value>();
		values.add(Value.get("new_val"));

		policy.filterExp = Exp.build(
			Exp.eq(
				HLLExp.getCount(Exp.hllBin(bin1)),
				HLLExp.getCount(HLLExp.add(HLLPolicy.Default, Exp.val(values), Exp.hllBin(bin2)))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.lt(
				HLLExp.getCount(Exp.hllBin(bin1)),
				HLLExp.getCount(HLLExp.add(HLLPolicy.Default, Exp.val(values), Exp.hllBin(bin2)))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}
}
