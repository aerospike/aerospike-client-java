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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.test.sync.TestSync;

public class TestOperate extends TestSync {
	@Test
	public void operate() {
		// Write initial record.
		Key key = new Key(args.namespace, args.set, "opkey");
		Bin bin1 = new Bin("optintbin", 7);
		Bin bin2 = new Bin("optstringbin", "string value");
		client.put(null, key, bin1, bin2);

		// Add integer, write new string and read record.
		Bin bin3 = new Bin(bin1.name, 4);
		Bin bin4 = new Bin(bin2.name, "new string");
		Record record = client.operate(null, key, Operation.add(bin3), Operation.put(bin4), Operation.get());
		assertBinEqual(key, record, bin3.name, 11);
		assertBinEqual(key, record, bin4);
	}

	@Test
	public void operateDelete() {
		// Write initial record.
		Key key = new Key(args.namespace, args.set, "opkey");
		Bin bin1 = new Bin("optintbin1", 1);

		client.put(null, key, bin1);

		// Read bin1 and then delete all.
		Record record = client.operate(null, key, Operation.get(bin1.name), Operation.delete());
		assertBinEqual(key, record, bin1.name, 1);

		// Verify record is gone.
		assertFalse(client.exists(null, key));

		// Rewrite record.
		Bin bin2 = new Bin("optintbin2", 2);

		client.put(null, key, bin1, bin2);

		// Read bin 1 and then delete all followed by a write of bin2.
		record = client.operate(null, key, Operation.get(bin1.name), Operation.delete(),
				Operation.put(bin2), Operation.get(bin2.name));
		assertBinEqual(key, record, bin1.name, 1);

		// Read record.
		record = client.get(null, key);
		assertBinEqual(key, record, bin2.name, 2);
		assertTrue(record.bins.size() == 1);
	}
}
