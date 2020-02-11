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

import static org.junit.Assert.fail;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.test.sync.TestSync;

public class TestPutGet extends TestSync {
	@Test
	public void putGet() {
		if (args.singleBin) {
			Key key = new Key(args.namespace, args.set, "putgetkey");
			Bin bin = new Bin("", "value");

			client.put(null, key, bin);
			Record record = client.get(null, key);
			assertBinEqual(key, record, bin);
		}
		else {
			Key key = new Key(args.namespace, args.set, "putgetkey");
			Bin bin1 = new Bin("bin1", "value1");
			Bin bin2 = new Bin("bin2", "value2");

			client.put(null, key, bin1, bin2);
			Record record = client.get(null, key);
			assertBinEqual(key, record, bin1);
			assertBinEqual(key, record, bin2);
		}
	}

	@Test
	public void getHeader() {
		Key key = new Key(args.namespace, args.set, "putgetkey");

		Record record = client.getHeader(null, key);
		assertRecordFound(key, record);

		// Generation should be greater than zero.  Make sure it's populated.
		if (record.generation == 0) {
			fail("Invalid record header: generation=" + record.generation + " expiration=" + record.expiration);
		}
	}
}
