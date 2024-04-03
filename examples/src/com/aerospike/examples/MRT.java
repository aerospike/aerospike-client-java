/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ReadModeSC;
import com.aerospike.client.tran.MrtCmd;
import com.aerospike.client.tran.Tran;

public class MRT extends Example {

	public MRT(Console console) {
		super(console);
	}

	/**
	 * Multi-record transaction.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "mrtkey");
		Bin bin1 = new Bin("bin", "val");

		//client.put(params.writePolicy, key, bin1);
		client.delete(params.writePolicy, key);

		Tran tran = client.tranBegin();

		Policy policy = client.copyReadPolicyDefault();
		policy.tran = tran;

		Record record = client.get(policy, key);
		if (record != null) {
			System.out.println("Record: " + record.toString());
		}
		else {
			System.out.println("Record is null");
		}

		client.put(params.writePolicy, key, bin1);
		//client.delete(params.writePolicy, key);

		//record = client.get(policy, key);
		//System.out.println("Record: " + record.toString());

		client.tranEnd(tran);
	}
}
