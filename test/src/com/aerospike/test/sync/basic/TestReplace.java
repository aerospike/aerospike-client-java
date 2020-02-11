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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;

public class TestReplace extends TestSync {
	@Test
	public void replace() {
		Key key = new Key(args.namespace, args.set, "replacekey");
		Bin bin1 = new Bin("bin1", "value1");
		Bin bin2 = new Bin("bin2", "value2");
		Bin bin3 = new Bin("bin3", "value3");

		client.put(null, key, bin1, bin2);

		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.REPLACE;
		client.put(policy, key, bin3);

		Record record = client.get(null, key);
		assertRecordFound(key, record);

		if (record.getValue(bin1.name) != null) {
			fail(bin1.name + " found when it should have been deleted.");
		}

		if (record.getValue(bin2.name) != null) {
			fail(bin2.name + " found when it should have been deleted.");
		}
		assertBinEqual(key, record, bin3);
	}

	@Test
	public void replaceOnly() {
		Key key = new Key(args.namespace, args.set, "replaceonlykey");
		Bin bin = new Bin("bin", "value");

		// Delete record if it already exists.
		client.delete(null, key);

		try {
			WritePolicy policy = new WritePolicy();
			policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
			client.put(policy, key, bin);

			fail("Failure. This command should have resulted in an error.");
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.KEY_NOT_FOUND_ERROR) {
				throw ae;
			}
		}
	}
}
