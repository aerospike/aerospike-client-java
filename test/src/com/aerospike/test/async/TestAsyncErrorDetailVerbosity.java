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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.ExpressionTrace;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.SubCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
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
	private static Key listKey;

	@BeforeClass
	public static void setup() {
		// Extended error-detail (subcode/message) plus the verbosity-3 expression
		// build trace (SERVER-1137). The SERVER-1137 feature branch is cut from the
		// 8.1.1 line and reports its base version as 8.1.1.0-start-*, so gate at
		// 8.1.1 rather than the 8.1.3 release that first shipped the base tier.
		org.junit.Assume.assumeTrue("Extended error-detail requires server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));

		WritePolicy wp = new WritePolicy();
		intKey = new Key(args.namespace, args.set, "edv-async-int-key");
		client.put(wp, intKey, new Bin(binName, 1));

		listKey = new Key(args.namespace, args.set, "edv-async-list-key");
		List<Value> seed = new ArrayList<>();
		seed.add(Value.get(10));
		client.put(wp, listKey, new Bin(binName, seed));
	}

	// AsyncOperateWrite — a write op that fails surfaces subcode + message.
	// A bounded ordered-list insert past the end yields OP_NOT_APPLICABLE with
	// AS_SUB_OPNOT_CDT_BOUNDED_LIST_OVERFLOW = 3.
	@Test
	public void asyncOperateWriteSurfacesDetail() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		ListPolicy bounded = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.INSERT_BOUNDED);

		AtomicReference<AerospikeException> caught = new AtomicReference<>();

		client.operate(eventLoop, new RecordListener() {
			public void onSuccess(Key key, Record record) {
				setError(new Exception("Expected OP_NOT_APPLICABLE, got success"));
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				caught.set(e);
				notifyComplete();
			}
		}, wp, listKey, ListOperation.insert(bounded, binName, 10, Value.get(5)));

		waitTillComplete();
		assertSubcode(caught.get(), ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_BOUNDED_LIST_OVERFLOW);
	}

	// AsyncDelete — generation mismatch surfaces the detail message. The status
	// is maximally specific, so the server omits the subcode (AS_SUB_NONE).
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
		assertSubcodeAbsent(caught.get(), ResultCode.GENERATION_ERROR, "generation mismatch");
	}

	// AsyncWrite — generation mismatch surfaces the detail message (AS_SUB_NONE).
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
		assertSubcodeAbsent(caught.get(), ResultCode.GENERATION_ERROR, "generation mismatch");
	}

	// AsyncTouch — generation mismatch surfaces the detail message (AS_SUB_NONE).
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
		assertSubcodeAbsent(caught.get(), ResultCode.GENERATION_ERROR, "generation mismatch");
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

	// ---------------------------------------------------------------------
	// Verbosity 3: expression build-failure trace (SERVER-1137), async paths.
	// A type-mismatched comparison fails to build on the server: as a filter_exp
	// read it yields "invalid metadata expression in request"; as an exp_write op,
	// "invalid expression in operation request". Both: PARAMETER_ERROR + NONE +
	// a build-phase trace. Assert presence/shape, not exact byte_offset/snippet.
	// ---------------------------------------------------------------------

	/** Expression with type-mismatched operands (int vs float) -> server build failure. */
	private static Exp badExp() {
		return Exp.eq(Exp.val(5), Exp.val(6.0));
	}

	@Test
	public void asyncFilterExpBuildFailureTrace() {
		Policy p = new Policy();
		p.errorDetailVerbosity = 3;
		p.filterExp = Exp.build(badExp());

		AtomicReference<AerospikeException> caught = new AtomicReference<>();

		client.get(eventLoop, new RecordListener() {
			public void onSuccess(Key key, Record record) {
				setError(new Exception("Expected PARAMETER_ERROR build failure, got success"));
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				caught.set(e);
				notifyComplete();
			}
		}, p, intKey);

		waitTillComplete();
		assertBuildTrace(caught.get(), "invalid metadata expression in request");
	}

	@Test
	public void asyncExpWriteBuildFailureTrace() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 3;

		AtomicReference<AerospikeException> caught = new AtomicReference<>();

		client.operate(eventLoop, new RecordListener() {
			public void onSuccess(Key key, Record record) {
				setError(new Exception("Expected PARAMETER_ERROR build failure, got success"));
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				caught.set(e);
				notifyComplete();
			}
		}, wp, intKey, ExpOperation.write(binName, Exp.build(badExp()), ExpWriteFlags.DEFAULT));

		waitTillComplete();
		assertBuildTrace(caught.get(), "invalid expression in operation request");
	}

	@Test
	public void asyncFilterExpBuildFailureVerbosity2HasNoTrace() {
		// Additive-superset check: same inducer at verbosity 2 -> message, no trace.
		Policy p = new Policy();
		p.errorDetailVerbosity = 2;
		p.filterExp = Exp.build(badExp());

		AtomicReference<AerospikeException> caught = new AtomicReference<>();

		client.get(eventLoop, new RecordListener() {
			public void onSuccess(Key key, Record record) {
				setError(new Exception("Expected PARAMETER_ERROR build failure, got success"));
				notifyComplete();
			}
			public void onFailure(AerospikeException e) {
				caught.set(e);
				notifyComplete();
			}
		}, p, intKey);

		waitTillComplete();

		AerospikeException ae = caught.get();
		org.junit.Assert.assertNotNull("Expected AerospikeException to be captured", ae);
		org.junit.Assert.assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
		org.junit.Assert.assertEquals(SubCode.NONE, ae.getSubCode());
		String msg = ae.getBaseMessage();
		org.junit.Assert.assertNotNull(msg);
		org.junit.Assert.assertTrue("Expected filter-build message in: " + msg,
			msg.contains("invalid metadata expression in request"));
		org.junit.Assert.assertNull("Verbosity 2 must surface NO expression trace", ae.getExpressionTrace());
	}

	/**
	 * Assert a verbosity-3 expression build failure: PARAMETER_ERROR + SubCode.NONE +
	 * the contextual message + a non-null build-phase trace.
	 */
	private static void assertBuildTrace(AerospikeException ae, String expectedSubstring) {
		org.junit.Assert.assertNotNull("Expected AerospikeException to be captured", ae);
		org.junit.Assert.assertEquals("Unexpected result code", ResultCode.PARAMETER_ERROR, ae.getResultCode());
		org.junit.Assert.assertEquals("Expected no subcode", SubCode.NONE, ae.getSubCode());

		String msg = ae.getBaseMessage();
		org.junit.Assert.assertNotNull("Expected server error message, got null. ae=" + ae, msg);
		org.junit.Assert.assertTrue("Expected '" + expectedSubstring + "' in: " + msg, msg.contains(expectedSubstring));

		ExpressionTrace t = ae.getExpressionTrace();
		org.junit.Assert.assertNotNull("Expected a non-null expression trace at verbosity 3", t);
		org.junit.Assert.assertEquals("Expected a build-phase trace", ExpressionTrace.PHASE_BUILD, t.getPhase());
	}

	/**
	 * Assert the server-supplied {@code (resultCode, subcode)} pair reached the
	 * async exception, including the first-class numeric subcode.
	 */
	private static void assertSubcode(AerospikeException ae, int expectedResultCode, int expectedSubcode) {
		org.junit.Assert.assertNotNull("Expected AerospikeException to be captured", ae);
		org.junit.Assert.assertEquals("Unexpected result code", expectedResultCode, ae.getResultCode());
		org.junit.Assert.assertEquals("Unexpected subcode", expectedSubcode, ae.getSubCode());

		String msg = ae.getBaseMessage();
		org.junit.Assert.assertNotNull("Expected server error message, got null. ae=" + ae, msg);
		org.junit.Assert.assertTrue("Expected 'subcode=" + expectedSubcode + "' in: " + msg,
			msg.contains("subcode=" + expectedSubcode));
	}

	/**
	 * Assert that the server surfaced a contextual message but NO subcode
	 * (AS_SUB_NONE): {@link AerospikeException#getSubCode()} is {@link SubCode#NONE}
	 * and the "(subcode=...)" suffix must never appear.
	 */
	private static void assertSubcodeAbsent(AerospikeException ae, int expectedResultCode, String expectedSubstring) {
		org.junit.Assert.assertNotNull("Expected AerospikeException to be captured", ae);
		org.junit.Assert.assertEquals("Unexpected result code", expectedResultCode, ae.getResultCode());
		org.junit.Assert.assertEquals("Expected no subcode", SubCode.NONE, ae.getSubCode());

		String msg = ae.getBaseMessage();
		org.junit.Assert.assertNotNull("Expected server error message, got null. ae=" + ae, msg);
		org.junit.Assert.assertTrue("Expected '" + expectedSubstring + "' in: " + msg, msg.contains(expectedSubstring));
		org.junit.Assert.assertFalse("Expected NO subcode suffix in: " + msg, msg.contains("subcode="));
	}

}
