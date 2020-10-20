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

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.exp.BitExp;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.operation.BitOverflowAction;
import com.aerospike.client.operation.BitPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.test.sync.TestSync;

public class TestBitExp extends TestSync {
	String binA = "A";
	Policy policy = new Policy();

	@Test
	public void callRead() {
		Key key = new Key(args.namespace, args.set, 5000);
		client.delete(null, key);

		Bin bin = new Bin(binA, new byte[] {0x01, 0x42, 0x03, 0x04, 0x05});
		client.put(null, key, bin);

		get(key);
		count(key);
		lscan(key);
		rscan(key);
		getInt(key);
	}

	@Test
	public void callModify() {
		Key key = new Key(args.namespace, args.set, 5001);
		client.delete(null, key);

		Bin bin = new Bin(binA, new byte[] {0x01, 0x42, 0x03, 0x04, 0x05});
		client.put(null, key, bin);

		resize(key);
		insert(key);
		remove(key);
		set(key);
		or(key);
		xor(key);
		and(key);
		not(key);
		lshift(key);
		rshift(key);
		add(key);
		subtract(key);
		setInt(key);
	}

	private void get(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA)),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA)),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA)),
				Exp.val(new byte[] {0x03})));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void count(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.count(Exp.val(16), Exp.val(8), Exp.blobBin(binA)),
				BitExp.count(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.count(Exp.val(16), Exp.val(8), Exp.blobBin(binA)),
				BitExp.count(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void lscan(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.lscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin(binA)),
				Exp.val(5)));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.lscan(Exp.val(0), Exp.val(8), Exp.val(true),
					BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))),
				Exp.val(5)));

		r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.lscan(Exp.val(0), Exp.val(8), Exp.val(true),
					BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))),
				Exp.val(5)));

		r = client.get(policy, key);
		assertRecordFound(key, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.lscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin(binA)),
				Exp.val(5)));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void rscan(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.rscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin(binA)),
				Exp.val(7)));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.rscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin(binA)),
				Exp.val(7)));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void getInt(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.getInt(Exp.val(32), Exp.val(8), true, Exp.blobBin(binA)),
				Exp.val(0x05)));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.getInt(Exp.val(32), Exp.val(8), true, Exp.blobBin(binA)),
				Exp.val(0x05)));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void resize(Key key) {
		Exp size = Exp.val(6);

		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.resize(BitPolicy.Default, size, 0, Exp.blobBin(binA)),
				BitExp.resize(BitPolicy.Default, size, 0, Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.resize(BitPolicy.Default, size, 0, Exp.blobBin(binA)),
				BitExp.resize(BitPolicy.Default, size, 0, Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void insert(Key key) {
		byte[] bytes = new byte[] {(byte)0xff};
		int expected = 0xff;

		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.getInt(Exp.val(8), Exp.val(8), false,
					BitExp.insert(BitPolicy.Default, Exp.val(1), Exp.val(bytes), Exp.blobBin(binA))),
				Exp.val(expected)));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.getInt(Exp.val(8), Exp.val(8), false,
					BitExp.insert(BitPolicy.Default, Exp.val(1), Exp.val(bytes), Exp.blobBin(binA))),
				Exp.val(expected)));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void remove(Key key) {
		int expected = 0x42;

		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.getInt(Exp.val(0), Exp.val(8), false,
					BitExp.remove(BitPolicy.Default, Exp.val(0), Exp.val(1), Exp.blobBin(binA))),
				Exp.val(expected)));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.getInt(Exp.val(0), Exp.val(8), false,
					BitExp.remove(BitPolicy.Default, Exp.val(0), Exp.val(1), Exp.blobBin(binA))),
				Exp.val(expected)));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void set(Key key) {
		byte[] bytes = new byte[] {(byte)0x80};

		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.set(BitPolicy.Default, Exp.val(31), Exp.val(1), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.set(BitPolicy.Default, Exp.val(31), Exp.val(1), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void or(Key key) {
		byte[] bytes = new byte[] {(byte)0x01};

		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.or(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.or(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void xor(Key key) {
		byte[] bytes = new byte[] {(byte)0x02};

		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.xor(BitPolicy.Default, Exp.val(0), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.xor(BitPolicy.Default, Exp.val(0), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void and(Key key) {
		byte[] bytes = new byte[] {(byte)0x01};

		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.and(BitPolicy.Default, Exp.val(16), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(0), Exp.val(8), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.and(BitPolicy.Default, Exp.val(16), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(0), Exp.val(8), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void not(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.not(BitPolicy.Default, Exp.val(6), Exp.val(1), Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.not(BitPolicy.Default, Exp.val(6), Exp.val(1), Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void lshift(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(0), Exp.val(6),
					BitExp.lshift(BitPolicy.Default, Exp.val(0), Exp.val(8), Exp.val(2), Exp.blobBin(binA))),
				BitExp.get(Exp.val(2), Exp.val(6), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(0), Exp.val(6),
					BitExp.lshift(BitPolicy.Default, Exp.val(0), Exp.val(8), Exp.val(2), Exp.blobBin(binA))),
				BitExp.get(Exp.val(2), Exp.val(6), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void rshift(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(26), Exp.val(6),
					BitExp.rshift(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(2), Exp.blobBin(binA))),
				BitExp.get(Exp.val(24), Exp.val(6), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(26), Exp.val(6),
					BitExp.rshift(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(2), Exp.blobBin(binA))),
				BitExp.get(Exp.val(24), Exp.val(6), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void add(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(16), Exp.val(8),
					BitExp.add(BitPolicy.Default, Exp.val(16), Exp.val(8), Exp.val(1), false, BitOverflowAction.FAIL, Exp.blobBin(binA))),
				BitExp.get(Exp.val(24), Exp.val(8), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(16), Exp.val(8),
					BitExp.add(BitPolicy.Default, Exp.val(16), Exp.val(8), Exp.val(1), false, BitOverflowAction.FAIL, Exp.blobBin(binA))),
				BitExp.get(Exp.val(24), Exp.val(8), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void subtract(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.subtract(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(1), false, BitOverflowAction.FAIL, Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.subtract(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(1), false, BitOverflowAction.FAIL, Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}

	private void setInt(Key key) {
		policy.filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.setInt(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(0x42), Exp.blobBin(binA))),
				BitExp.get(Exp.val(8), Exp.val(8), Exp.blobBin(binA))));

		Record r = client.get(policy, key);
		assertEquals(null, r);

		policy.filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.setInt(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(0x42), Exp.blobBin(binA))),
				BitExp.get(Exp.val(8), Exp.val(8), Exp.blobBin(binA))));

		r = client.get(policy, key);
		assertRecordFound(key, r);
	}
}
