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
package com.aerospike.examples;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Txn;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class Transaction extends Example {
	public Transaction(Console console) {
		super(console);
	}

	/**
	 * Multi-record transaction.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		txnReadWrite(client, params);
	}

	private void txnReadWrite(IAerospikeClient client, Parameters params) {
		Txn txn = new Txn();
		console.info("Begin txn: " + txn.getId());

		try {
			WritePolicy wp = client.copyWritePolicyDefault();
			wp.txn = txn;

			console.info("Run put");
			Key key1 = new Key(params.namespace, params.set, 1);
			client.put(wp, key1, new Bin("a", "val1"));

			console.info("Run another put");
			Key key2 = new Key(params.namespace, params.set, 2);
			client.put(wp, key2, new Bin("b", "val2"));

			console.info("Run get");
			Policy p = client.copyReadPolicyDefault();
			p.txn = txn;

			Key key3 = new Key(params.namespace, params.set, 3);
			client.get(p, key3);

			console.info("Run delete");
			WritePolicy dp = client.copyWritePolicyDefault();
			dp.txn = txn;
			dp.durableDelete = true;  // Required when running delete in a transaction.
			client.delete(dp, key3);
		}
		catch (Throwable t) {
			// Abort and rollback transaction if any errors occur.
			console.info("Abort txn: " + txn.getId());
			client.abort(txn);
			throw t;
		}

		// Commit transaction. If the verify step of the commit fails,
		// the transaction is automatically aborted.
		console.info("Commit txn: " + txn.getId());
		client.commit(txn);
	}
}
