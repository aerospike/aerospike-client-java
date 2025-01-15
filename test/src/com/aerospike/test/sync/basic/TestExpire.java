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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;
import com.aerospike.test.sync.TestSync;

public class TestExpire extends TestSync {
	private static final String binName = "expirebin";

	@Test
	public void expire() {
		org.junit.Assume.assumeTrue(args.hasTtl);

		Key key  = new Key(args.namespace, args.set, "expirekey1");
		Bin bin  = new Bin(binName, "expirevalue");

		// Specify that record expires 2 seconds after it's written.
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 2;
		client.put(writePolicy, key, bin);

		// Read the record before it expires, showing it is there.
		Record record = client.get(null, key, bin.name);
		assertBinEqual(key, record, bin);

		// Read the record after it expires, showing it's gone.
		Util.sleep(3 * 1000);
		record = client.get(null, key, bin.name);
		assertNull(record);
	}
	@Test
	public void noExpire() {
		Key key = new Key(args.namespace, args.set, "expirekey2");
		Bin bin = new Bin(binName, "noexpirevalue");

		// Specify that record NEVER expires.
		// The "Never Expire" value is -1, or 0xFFFFFFFF.
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = -1;
		client.put(writePolicy, key, bin);

		// Read the record, showing it is there.
		Record record = client.get(null, key, bin.name);
		assertBinEqual(key, record, bin);

		// Read this Record after the Default Expiration, showing it is still there.
		// We should have set the Namespace TTL at 5 sec.
		Util.sleep(10 * 1000);
		record = client.get(null, key, bin.name);
		assertNotNull(record);
	}

	@Test
	public void resetReadTtl() {
		org.junit.Assume.assumeTrue(args.hasTtl);

		Key key  = new Key(args.namespace, args.set, "expirekey3");
		Bin bin  = new Bin(binName, "expirevalue");

		// Specify that record expires 2 seconds after it's written.
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 2;
		client.put(writePolicy, key, bin);

		// Read the record before it expires and reset read ttl.
		Util.sleep(1000);
		Policy readPolicy = new Policy();
		readPolicy.readTouchTtlPercent = 80;
		Record record = client.get(readPolicy, key, bin.name);
		assertBinEqual(key, record, bin);

		// Read the record again, but don't reset read ttl.
		Util.sleep(1000);
		readPolicy.readTouchTtlPercent = -1;
		record = client.get(readPolicy, key, bin.name);
		assertBinEqual(key, record, bin);

		// Read the record after it expires, showing it's gone.
		Util.sleep(2000);
		record = client.get(null, key, bin.name);
		assertNull(record);
	}
}
