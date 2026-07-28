/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.examples;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class DeleteBin extends Example {

	/**
	 * Drop a bin from a record.
	 */
	@Override
	public void runExample() throws Exception {
		Key key = new Key(namespace(), set(), "delbinkey");
		String binName1 = "bin1";
		String binName2 = "bin2";

		console.info("Delete one bin in the record.");
		Bin bin1 = Bin.asNull(binName1); // Set bin value to null to drop bin.
		client().put(writePolicy(), key, bin1);

		console.info("Read record.");
		Record record = client().get(readPolicy(), key, binName1, binName2);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}
		console.info("Record after delete: namespace=%s set=%s key=%s bin1=%s bin2=%s",
			key.namespace, key.setName, key.userKey, record.getValue(binName1), record.getValue(binName2));
	}
}
