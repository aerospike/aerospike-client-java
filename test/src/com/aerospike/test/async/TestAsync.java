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
package com.aerospike.test.async;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.test.SuiteAsync;
import com.aerospike.test.util.TestBase;

public class TestAsync extends TestBase {
	protected static IAerospikeClient client = SuiteAsync.client;
	protected static EventLoop eventLoop = SuiteAsync.eventLoop;
	private static boolean DestroyClient = false;

	@BeforeClass
	public static void init() {
		if (client == null) {
			SuiteAsync.init();
			client = SuiteAsync.client;
			eventLoop = SuiteAsync.eventLoop;
			DestroyClient = true;
		}
	}

	@AfterClass
	public static void destroy() {
		if (DestroyClient) {
			SuiteAsync.destroy();
		}
	}

	private AsyncMonitor monitor = new AsyncMonitor();

	public boolean assertBinEqual(Key key, Record record, Bin bin) {
		if (! assertRecordFound(key, record)) {
			return false;
		}

		Object received = record.getValue(bin.name);
		Object expected = bin.value.getObject();

		if (received == null || ! received.equals(expected)) {
			monitor.setError(new Exception("Data mismatch: Expected " + expected + ". Received " + received));
			return false;
		}
		return true;
	}

	public boolean assertBinEqual(Key key, Record record, String binName, Object expected) {
		if (! assertRecordFound(key, record)) {
			return false;
		}

		Object received = record.getValue(binName);

		if (received == null || ! received.equals(expected)) {
			monitor.setError(new Exception("Data mismatch: Expected " + expected + ". Received " + received));
			return false;
		}
		return true;
	}

	public boolean assertBinEqual(Key key, Record record, String binName, int expected) {
		if (! assertRecordFound(key, record)) {
			return false;
		}

		int received = record.getInt(binName);

		if (received != expected) {
			monitor.setError(new Exception("Data mismatch: Expected " + expected + ". Received " + received));
			return false;
		}
		return true;
	}

	public boolean assertRecordFound(Key key, Record record) {
		if (record == null) {
			monitor.setError(new Exception("Failed to get: namespace=" + args.namespace + " set=" + args.set + " key=" + key.userKey));
			return false;
		}
		return true;
 	}

	public boolean assertRecordNotFound(Key key, Record record) {
		if (record != null) {
			monitor.setError(new Exception("Record should not exist: namespace=" + args.namespace + " set=" + args.set + " key=" + key.userKey));
			return false;
		}
		return true;
	}

	public boolean assertBetween(long begin, long end, long value) {
		if (! (value >= begin && value <= end)) {
			monitor.setError(new Exception("Range " + value + " not between " + begin + " and " + end));
			return false;
		}
		return true;
	}

	public boolean assertEquals(long expected, long received) {
		if (expected != received) {
			monitor.setError(new Exception("Data mismatch: Expected " + expected + ". Received " + received));
			return false;
		}
		return true;
	}

	public boolean assertEquals(Object expected, Object received) {
		if (! expected.equals(received)) {
			monitor.setError(new Exception("Data mismatch: Expected " + expected + ". Received " + received));
			return false;
		}
		return true;
	}

	public boolean assertEquals(boolean expected, boolean received) {
		if (expected != received) {
			monitor.setError(new Exception("Data mismatch: Expected " + expected + ". Received " + received));
			return false;
		}
		return true;
	}

	public boolean assertGreaterThanZero(long value) {
		if (value <= 0) {
			monitor.setError(new Exception("Value not greater than zero"));
			return false;
		}
		return true;
	}

	public boolean assertValidExpiration(long value) {
		if (value < 0) {
			monitor.setError(new Exception("Invalid expiration"));
			return false;
		}
		return true;
	}

	public boolean assertNotNull(Object obj) {
		if (obj == null) {
			monitor.setError(new Exception("Object is null"));
			return false;
		}
		return true;
	}

	public boolean assertNull(Object obj) {
		if (obj != null) {
			monitor.setError(new Exception("Object is not null"));
			return false;
		}
		return true;
	}

	public boolean assertTrue(boolean b) {
		if (! b) {
			monitor.setError(new Exception("Value is false"));
			return false;
		}
		return true;
	}

	public boolean assertBatchEqual(Key[] keys, Record[] recs, String binName, int expected) {
		for (int i = 0; i < keys.length; i++) {
			Record rec = recs[i];

			if (rec == null) {
				monitor.setError(new Exception("recs[" + i + "] is null"));
				return false;
			}

			int received = rec.getInt(binName);

			if (expected != received) {
				monitor.setError(new Exception("Data mismatch: Expected " + expected + ". Received[" + i + "] " + received));
				return false;
			}
		}
		return true;
	}

	public void setError(Throwable t) {
		monitor.setError(t);
	}

	public void waitTillComplete() {
		monitor.waitTillComplete();
	}

	public void notifyComplete() {
		monitor.notifyComplete();
	}
}
