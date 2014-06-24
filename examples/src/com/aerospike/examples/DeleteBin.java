/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class DeleteBin extends Example {

	public DeleteBin(Console console) {
		super(console);
	}

	/**
	 * Drop a bin from a record.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (params.singleBin) {
			console.info("Delete bin is not applicable to single bin servers.");			
			return;
		}
			
		console.info("Write multi-bin record.");
		Key key = new Key(params.namespace, params.set, "delbinkey");
		String binName1 = params.getBinName("bin1");
		String binName2 = params.getBinName("bin2");		
		Bin bin1 = new Bin(binName1, "value1");
		Bin bin2 = new Bin(binName2, "value2");
		client.put(params.writePolicy, key, bin1, bin2);
	
		console.info("Delete one bin in the record.");
		bin1 = Bin.asNull(binName1); // Set bin value to null to drop bin.
		client.put(params.writePolicy, key, bin1);

		console.info("Read record.");		
		Record record = client.get(params.policy, key, bin1.name, bin2.name, "bin3");

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s", 
				key.namespace, key.setName, key.userKey));
		}

		for (Map.Entry<String,Object> entry : record.bins.entrySet()) {
			console.info("Received: namespace=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, entry.getKey(), entry.getValue());			
		}
		
		boolean valid = true;
		
		if (record.getValue("bin1") != null) {
			console.error("bin1 still exists.");
			valid = false;
		}
		
		Object v2 = record.getValue("bin2");
		
		if (v2 == null || ! v2.equals("value2")) {
			console.error("bin2 value mismatch.");
			valid = false;
		}
		
		if (valid) {
			console.info("Bin delete successful");
		}
	}
}
