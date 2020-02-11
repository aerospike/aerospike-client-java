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
package com.aerospike.test.sync;

import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.test.SuiteSync;
import com.aerospike.test.util.TestBase;

public class TestSync extends TestBase {
	protected static AerospikeClient client = SuiteSync.client;
	private static boolean DestroyClient = false;

	@BeforeClass
	public static void init() {
		if (client == null) {
			SuiteSync.init();
			client = SuiteSync.client;
			DestroyClient = true;
		}
	}

	@AfterClass
	public static void destroy() {
		if (DestroyClient) {
			SuiteSync.destroy();
		}
	}

	public void assertRecordFound(Key key, Record record) {
		if (record == null) {
			fail("Failed to get: namespace=" + args.namespace + " set=" + args.set + " key=" + key.userKey);
		}
 	}

	public void assertBinEqual(Key key, Record record, Bin bin) {
		assertRecordFound(key, record);

		Object received = record.getValue(bin.name);
		Object expected = bin.value.getObject();

		if (received == null || ! received.equals(expected)) {
			fail("Data mismatch: Expected " + expected + ". Received " + received);
		}
	}

	public void assertBinEqual(Key key, Record record, String binName, Object expected) {
		assertRecordFound(key, record);

		Object received = record.getValue(binName);

		if (received == null || ! received.equals(expected)) {
			fail("Data mismatch: Expected " + expected + ". Received " + received);
		}
	}

	public void assertBinEqual(Key key, Record record, String binName, int expected) {
		assertRecordFound(key, record);

		int received = record.getInt(binName);

		if (received != expected) {
			fail("Data mismatch: Expected " + expected + ". Received " + received);
		}
	}
}
