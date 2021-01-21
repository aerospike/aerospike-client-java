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

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.policy.Policy;
import com.aerospike.test.sync.TestSync;

public class TestListExp extends TestSync {
	String binA = "A";
	String binB = "B";
	String binC = "C";

	Key keyA = new Key(args.namespace, args.set, binA);
	Key keyB = new Key(args.namespace, args.set, binB);

	Policy policy;

	@Before
	public void setUp() throws Exception {
		client.delete(null, keyA);
		client.delete(null, keyB);
		policy = new Policy();
	}

	@Test
	public void modifyWithContext() {
		List<Value> listSubA = new ArrayList<Value>();
		listSubA.add(Value.get("e"));
		listSubA.add(Value.get("d"));
		listSubA.add(Value.get("c"));
		listSubA.add(Value.get("b"));
		listSubA.add(Value.get("a"));

		List<Value> listA = new ArrayList<Value>();
		listA.add(Value.get("a"));
		listA.add(Value.get("b"));
		listA.add(Value.get("c"));
		listA.add(Value.get("d"));
		listA.add(Value.get(listSubA));

		List<Value> listB = new ArrayList<Value>();
		listB.add(Value.get("x"));
		listB.add(Value.get("y"));
		listB.add(Value.get("z"));

		client.operate(null, keyA,
			ListOperation.appendItems(ListPolicy.Default, binA, listA),
			ListOperation.appendItems(ListPolicy.Default, binB, listB),
			Operation.put(new Bin(binC, "M"))
			);

		CTX ctx = CTX.listIndex(4);
		Record record;
		List<?> result;

		policy.filterExp = Exp.build(
			Exp.eq(
				ListExp.size(
					// Temporarily append binB/binC to binA in expression.
					ListExp.appendItems(ListPolicy.Default, Exp.listBin(binB),
						ListExp.append(ListPolicy.Default, Exp.stringBin(binC), Exp.listBin(binA), ctx),
						ctx),
					ctx),
				Exp.val(9)));

		record = client.get(policy, keyA, binA);
		assertRecordFound(keyA, record);

		result = record.getList(binA);
		assertEquals(5, result.size());

		policy.filterExp = Exp.build(
			Exp.eq(
				ListExp.size(
					// Temporarily append local listB and local "M" string to binA in expression.
					ListExp.appendItems(ListPolicy.Default, Exp.val(listB),
						ListExp.append(ListPolicy.Default, Exp.val("M"), Exp.listBin(binA), ctx),
						ctx),
					ctx),
				Exp.val(9)));

		record = client.get(policy, keyA, binA);
		assertRecordFound(keyA, record);

		result = record.getList(binA);
		assertEquals(5, result.size());
	}
}
