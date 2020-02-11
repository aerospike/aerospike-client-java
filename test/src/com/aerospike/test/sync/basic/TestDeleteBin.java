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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.test.sync.TestSync;

public class TestDeleteBin extends TestSync {
	@Test
	public void deleteBin() {
		Key key = new Key(args.namespace, args.set, "delbinkey");
		String binName1 = args.getBinName("bin1");
		String binName2 = args.getBinName("bin2");
		Bin bin1 = new Bin(binName1, "value1");
		Bin bin2 = new Bin(binName2, "value2");
		client.put(null, key, bin1, bin2);

		bin1 = Bin.asNull(binName1); // Set bin value to null to drop bin.
		client.put(null, key, bin1);

		Record record = client.get(null, key, bin1.name, bin2.name, "bin3");
		assertRecordFound(key, record);

		if (record.getValue("bin1") != null) {
			fail("bin1 still exists.");
		}

		Object v2 = record.getValue("bin2");
		assertNotNull(v2);
		assertEquals("value2", v2);
	}
}
