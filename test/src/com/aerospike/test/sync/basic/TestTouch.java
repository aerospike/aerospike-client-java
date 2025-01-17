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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;
import com.aerospike.test.sync.TestSync;

public class TestTouch extends TestSync {
	@Test
	public void touchOperate() {
		org.junit.Assume.assumeTrue(args.hasTtl);

		Key key = new Key(args.namespace, args.set, "touchOperate");
		Bin bin = new Bin("touchbin", "touchvalue");

		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 1;
		client.put(writePolicy, key, bin);

		writePolicy.expiration = 2;
		Record record = client.operate(writePolicy, key, Operation.touch(), Operation.getHeader());
		assertRecordFound(key, record);
		assertNotEquals(0, record.expiration);

		Util.sleep(1000);

		record = client.get(null, key, bin.name);
		assertRecordFound(key, record);

		Util.sleep(3000);

		record = client.get(null, key, bin.name);
		assertNull(record);
	}

	@Test
	public void touch() {
		org.junit.Assume.assumeTrue(args.hasTtl);

		Key key = new Key(args.namespace, args.set, "touch");
		Bin bin = new Bin("touchbin", "touchvalue");

		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 1;
		client.put(writePolicy, key, bin);

		writePolicy.expiration = 2;
		client.touch(writePolicy, key);

		Util.sleep(1000);

		Record record = client.getHeader(writePolicy, key);
		assertRecordFound(key, record);
		assertNotEquals(0, record.expiration);

		Util.sleep(3000);

		record = client.getHeader(null, key);
		assertNull(record);
	}

	@Test
	public void touched() {
		org.junit.Assume.assumeTrue(args.hasTtl);

		Key key = new Key(args.namespace, args.set, "touched");

		client.delete(null, key);

		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 10;
		boolean rv = client.touched(writePolicy, key);
		assertEquals(false, rv);

		Bin bin = new Bin("touchbin", "touchvalue");
		client.put(writePolicy, key, bin);

		rv = client.touched(writePolicy, key);
		assertEquals(true, rv);
	}
}
