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
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;

public class TestGeneration extends TestSync {
	@Test
	public void generation() {
		Key key = new Key(args.namespace, args.set, "genkey");
		String binName = args.getBinName("genbin");

		// Delete record if it already exists.
		client.delete(null, key);

		// Set some values for the same record.
		Bin bin = new Bin(binName, "genvalue1");

		client.put(null, key, bin);

		bin = new Bin(binName, "genvalue2");

		client.put(null, key, bin);

		// Retrieve record and its generation count.
		Record record = client.get(null, key, bin.name);
		assertBinEqual(key, record, bin);

		// Set record and fail if it's not the expected generation.
		bin = new Bin(binName, "genvalue3");

		WritePolicy writePolicy = new WritePolicy();
		writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		writePolicy.generation = record.generation;
		client.put(writePolicy, key, bin);

		// Set record with invalid generation and check results .
		bin = new Bin(binName, "genvalue4");
		writePolicy.generation = 9999;

		try {
			client.put(writePolicy, key, bin);
			fail("Should have received generation error instead of success.");
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.GENERATION_ERROR) {
				fail("Unexpected return code: namespace=" + key.namespace + " set=" + key.setName +
					" key=" + key.userKey + " bin=" + bin.name + " value=" + bin.value +
					" code=" + ae.getResultCode());
			}
		}

		// Verify results.
		record = client.get(null, key, bin.name);
		assertBinEqual(key, record, bin.name, "genvalue3");
	}
}
