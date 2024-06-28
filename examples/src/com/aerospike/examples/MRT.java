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
import com.aerospike.client.Record;
import com.aerospike.client.Tran;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class MRT extends Example {
	public MRT(Console console) {
		super(console);
	}

	/**
	 * Multi-record transaction.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		tranReadWrite(client, params);
	}

	private void tranReadWrite(IAerospikeClient client, Parameters params) {
		Tran tran = client.tranBegin();
		System.out.println("Begin tran: " + tran.getId());

		try {
			WritePolicy wp = client.copyWritePolicyDefault();
			wp.tran = tran;

			Key key1 = new Key(params.namespace, params.set, 1);
			client.put(wp, key1, new Bin("a", "val1"));

			Key key2 = new Key(params.namespace, params.set, 2);
			client.put(wp, key2, new Bin("b", "val2"));

			Policy p = client.copyReadPolicyDefault();
			p.tran = tran;

			Key key3 = new Key(params.namespace, params.set, 3);
			Record rec = client.get(p, key3);

			WritePolicy dp = client.copyWritePolicyDefault();
			dp.tran = tran;
			dp.durableDelete = true;  // Required when running delete in a MRT.
			client.delete(dp, key3);
		}
		catch (Throwable t) {
			// Abort and rollback MRT (multi-record transaction) if any errors occur.
			client.tranAbort(tran);
			throw t;
		}

		System.out.println("Commit tran: " + tran.getId());
		client.tranCommit(tran);
	}
}
