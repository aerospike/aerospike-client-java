/*
 * Copyright 2012-2018 Aerospike, Inc.
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

import org.junit.Test;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.util.Version;
import com.aerospike.test.sync.TestSync;

public class TestAdd extends TestSync {
	@Test
	public void add() {
		Key key = new Key(args.namespace, args.set, "addkey");
		String binName = args.getBinName("addbin");

		// Delete record if it already exists.
		client.delete(null, key);

		// Perform some adds and check results.
		Bin bin = new Bin(binName, 10);
		client.add(null, key, bin);

		bin = new Bin(binName, 5);
		client.add(null, key, bin);

		Record record = client.get(null, key, bin.name);
		assertBinEqual(key, record, bin.name, 15);

		// Test add and get combined.
		bin = new Bin(binName, 30);
		record = client.operate(null, key, Operation.add(bin), Operation.get(bin.name));
		assertBinEqual(key, record, bin.name, 45);
	}

	@Test
	public void addNullValue() {	
		Version version = Version.getServerVersion(client, null);
		
		// Do not run on servers < 3.6.1
		if (version.isLess(3, 6, 1)) {
			return;
		}
		
		Key key = new Key(args.namespace, args.set, "addkey");
		String binName = args.getBinName("addbin");
		
		// Delete record if it already exists.
		client.delete(null, key);
		
		Bin bin;
		
		// verify correct exception for previous server crash
		try {
			bin = new Bin(binName, (Long)null);
			client.add(null, key, bin);
			fail("add with null value should not have succeeded");
		}
		catch (AerospikeException ae) {
			assertEquals(ae.getMessage(), "Error Code 4: Parameter error");
		}
		catch (Exception e) {
			fail("add with null value should have thrown AerospikeException");
		}
	}
}
