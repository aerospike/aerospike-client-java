/*
 * Copyright 2012-2026 Aerospike, Inc.
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

import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Validates that server-supplied error details (subcode + message) reach
 * AerospikeException through every async command path.
 */
public class TestAsyncErrorDetailVerbosity extends TestAsync {
	private static final String binName = "edv-bin";
	private static Key intKey;

	@BeforeClass
	public static void setup() {
		WritePolicy wp = new WritePolicy();
		intKey = new Key(args.namespace, args.set, "edv-async-int-key");
		client.put(wp, intKey, new Bin(binName, 1));
	}

	// AsyncOperateWrite — type mismatch surfaces subcode + message
	@Test
	public void asyncOperateWriteSurfacesDetail() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		AtomicReference<AerospikeException> caught = new AtomicReference<>();

		client.operate(eventLoop, new RecordListener() {
			public void onSuccess(Key key, Record record) {
				setError(new Exception("Expected BIN_TYPE_ERROR, got success"));
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				caught.set(e);
				notifyComplete();
			}
		}, wp, intKey, Operation.append(new Bin(binName, "bad-append")));

		waitTillComplete();
		assertDetail(caught.get(), ResultCode.BIN_TYPE_ERROR, "cannot append", "subcode=");
	}

	// AsyncDelete — generation mismatch surfaces subcode + message
	@Test
	public void asyncDeleteSurfacesDetail() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		wp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		wp.generation = 777;

		AtomicReference<AerospikeException> caught = new AtomicReference<>();

		client.delete(eventLoop, new DeleteListener() {
			public void onSuccess(Key key, boolean existed) {
				setError(new Exception("Expected GENERATION_ERROR, got success"));
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				caught.set(e);
				notifyComplete();
			}
		}, wp, intKey);

		waitTillComplete();
		assertDetail(caught.get(), ResultCode.GENERATION_ERROR, "delete generation mismatch", "subcode=");
	}

	// AsyncWrite — generation mismatch surfaces subcode + message
	@Test
	public void asyncWriteSurfacesDetail() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		wp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		wp.generation = 777;

		AtomicReference<AerospikeException> caught = new AtomicReference<>();

		client.put(eventLoop, new WriteListener() {
			public void onSuccess(Key key) {
				setError(new Exception("Expected GENERATION_ERROR, got success"));
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				caught.set(e);
				notifyComplete();
			}
		}, wp, intKey, new Bin(binName, 2));

		waitTillComplete();
		assertDetail(caught.get(), ResultCode.GENERATION_ERROR, "subcode=");
	}

	// AsyncTouch — generation mismatch surfaces subcode + message
	@Test
	public void asyncTouchSurfacesDetail() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		wp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		wp.generation = 777;

		AtomicReference<AerospikeException> caught = new AtomicReference<>();

		client.touch(eventLoop, new WriteListener() {
			public void onSuccess(Key key) {
				setError(new Exception("Expected GENERATION_ERROR, got success"));
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				caught.set(e);
				notifyComplete();
			}
		}, wp, intKey);

		waitTillComplete();
		assertDetail(caught.get(), ResultCode.GENERATION_ERROR, "subcode=");
	}

	// AsyncExists — uses Policy (not WritePolicy). Server should not error on plain exists;
	// just verifies the configured verbosity does not break the happy path.
	@Test
	public void asyncExistsVerbositySetHappyPath() {
		Policy p = new Policy();
		p.errorDetailVerbosity = 2;

		client.exists(eventLoop, new ExistsListener() {
			public void onSuccess(Key key, boolean exists) {
				if (! exists) {
					setError(new Exception("Expected record to exist"));
				}
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, p, intKey);

		waitTillComplete();
	}

	// AsyncRead — verifies happy path with verbosity set
	@Test
	public void asyncReadVerbositySetHappyPath() {
		Policy p = new Policy();
		p.errorDetailVerbosity = 2;

		client.get(eventLoop, new RecordListener() {
			public void onSuccess(Key key, Record record) {
				if (record == null || record.getInt(binName) != 1) {
					setError(new Exception("Unexpected record: " + record));
				}
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, p, intKey);

		waitTillComplete();
	}

	// AsyncReadHeader — verifies happy path with verbosity set
	@Test
	public void asyncReadHeaderVerbositySetHappyPath() {
		Policy p = new Policy();
		p.errorDetailVerbosity = 2;

		client.getHeader(eventLoop, new RecordListener() {
			public void onSuccess(Key key, Record record) {
				if (record == null) {
					setError(new Exception("Expected header"));
				}
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, p, intKey);

		waitTillComplete();
	}

	private static void assertDetail(AerospikeException ae, int expectedResultCode, String... expectedSubstrings) {
		org.junit.Assert.assertNotNull("Expected AerospikeException to be captured", ae);
		org.junit.Assert.assertEquals("Unexpected result code", expectedResultCode, ae.getResultCode());

		String msg = ae.getBaseMessage();
		org.junit.Assert.assertNotNull("Expected server error message, got null. ae=" + ae, msg);

		for (String expected : expectedSubstrings) {
			org.junit.Assert.assertTrue("Expected '" + expected + "' in: " + msg, msg.contains(expected));
		}
	}

}
