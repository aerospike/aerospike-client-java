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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.SubCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;

/**
 * Live-server verification that the extended error detail (sub-code / message) is
 * surfaced per row on batch commands, mirroring the single-record coverage in
 * {@link TestErrorDetailVerbosity} / {@link TestErrorDetailSubcode}.
 *
 * <p>The detail is batch-wide opt-in: the parent {@link BatchPolicy#errorDetailVerbosity}
 * is folded into each row's info4 verbosity bits, and the server attaches a per-row
 * error detail (field 45) that the client decodes onto {@link BatchRecord#serverMessage},
 * {@link BatchRecord#subCode}, and {@link BatchRecord#expTrace}. Requires an 8.1.3+ server.
 */
public class TestErrorDetailBatch extends TestSync {

	private static final String binName = "edb-bin";
	private static Key listKey;

	@BeforeClass
	public static void setup() {
		org.junit.Assume.assumeTrue("Extended error-detail requires server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));

		listKey = new Key(args.namespace, args.set, "edb-list-key");

		List<Value> seed = new ArrayList<>();
		seed.add(Value.get(10));
		seed.add(Value.get(20));
		seed.add(Value.get(30));
		client.put(new WritePolicy(), listKey, new Bin(binName, seed));
	}

	/**
	 * With verbosity opted in, an erroring batch row surfaces the server sub-code and
	 * message, while a successful row in the same batch carries no detail.
	 */
	@Test
	public void testBatchRowSurfacesSubcode() {
		BatchPolicy bp = new BatchPolicy(client.getBatchParentPolicyWriteDefault());
		bp.errorDetailVerbosity = 2;

		// Error row: list get with an out-of-bounds index -> OP_NOT_APPLICABLE + CDT sub-code.
		BatchRead errRow = new BatchRead(listKey, new Operation[] {
			ListOperation.get(binName, 99)
		});
		// Success row: valid list size op on the same key.
		BatchRead okRow = new BatchRead(listKey, new Operation[] {
			ListOperation.size(binName)
		});

		List<BatchRecord> records = new ArrayList<>();
		records.add(errRow);
		records.add(okRow);

		client.operate(bp, records);

		// Error row carries the server sub-code and formatted message.
		assertEquals("Unexpected result code", ResultCode.OP_NOT_APPLICABLE, errRow.resultCode);
		assertEquals("Unexpected subcode", SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, errRow.subCode);
		assertNotNull("Expected server error message", errRow.serverMessage);
		assertTrue("Expected 'subcode=" + SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS + "' in: " + errRow.serverMessage,
			errRow.serverMessage.contains("subcode=" + SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS));

		// Success row carries no error detail.
		assertEquals(ResultCode.OK, okRow.resultCode);
		assertNull("OK row should have no server message", okRow.serverMessage);
		assertEquals("OK row should have no subcode", SubCode.NONE, okRow.subCode);
		assertNull("OK row should have no expression trace", okRow.expTrace);
	}

	/**
	 * Without opting in (default verbosity 0), the row still reports the result code but
	 * the server attaches no extended detail, so sub-code/message stay cleared.
	 */
	@Test
	public void testBatchRowNoDetailWhenVerbosityOff() {
		BatchPolicy bp = new BatchPolicy(client.getBatchParentPolicyWriteDefault());
		bp.errorDetailVerbosity = 0;

		BatchRead errRow = new BatchRead(listKey, new Operation[] {
			ListOperation.get(binName, 99)
		});

		List<BatchRecord> records = new ArrayList<>();
		records.add(errRow);

		client.operate(bp, records);

		assertEquals("Unexpected result code", ResultCode.OP_NOT_APPLICABLE, errRow.resultCode);
		assertEquals("No detail expected at verbosity 0", SubCode.NONE, errRow.subCode);
		assertNull("No detail expected at verbosity 0", errRow.serverMessage);
		assertNull("No detail expected at verbosity 0", errRow.expTrace);
	}

	/**
	 * A batch containing exactly one record is dispatched down the single-key path
	 * ({@code BatchSingle}/{@code AsyncBatchSingle}, chosen whenever a node receives a
	 * single offset), which decodes the same field-45 detail but must surface it onto
	 * the {@link BatchRecord} itself. This read case exercises {@code BatchSingle.ReadRecord}.
	 * Regression guard: before the fix the detail was decoded and then dropped here, so
	 * sub-code/message came back cleared even with verbosity opted in.
	 */
	@Test
	public void testSingleKeyBatchReadSurfacesSubcode() {
		BatchPolicy bp = new BatchPolicy(client.getBatchParentPolicyWriteDefault());
		bp.errorDetailVerbosity = 2;

		BatchRead errRow = new BatchRead(listKey, new Operation[] {
			ListOperation.get(binName, 99)
		});

		// Single-record batch -> offsetsSize == 1 -> single-key path.
		List<BatchRecord> records = new ArrayList<>();
		records.add(errRow);

		client.operate(bp, records);

		assertEquals("Unexpected result code", ResultCode.OP_NOT_APPLICABLE, errRow.resultCode);
		assertEquals("Single-key read row lost its subcode",
			SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, errRow.subCode);
		assertNotNull("Single-key read row lost its server message", errRow.serverMessage);
		assertTrue("Expected 'subcode=" + SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS + "' in: " + errRow.serverMessage,
			errRow.serverMessage.contains("subcode=" + SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS));
	}

	/**
	 * Single-key write variant: a one-record batch write is dispatched to
	 * {@code BatchSingle.OperateBatchRecord}, proving the detail reaches the
	 * {@link BatchRecord} on the write path too. Appending a string to an integer bin is a
	 * write op that fails with BIN_TYPE_ERROR and a server message. (This particular error
	 * carries no sub-code on the server, so the message is the detail asserted here; the read
	 * case above covers sub-code surfacing.)
	 */
	@Test
	public void testSingleKeyBatchWriteSurfacesMessage() {
		Key intKey = new Key(args.namespace, args.set, "edb-int-key");
		client.put(new WritePolicy(), intKey, new Bin("i", 1));

		BatchPolicy bp = new BatchPolicy(client.getBatchParentPolicyWriteDefault());
		bp.errorDetailVerbosity = 2;

		// Append (a write op) to an integer bin -> BIN_TYPE_ERROR with a server message.
		BatchWrite errRow = new BatchWrite(intKey, new Operation[] {
			Operation.append(new Bin("i", "bad-append"))
		});

		List<BatchRecord> records = new ArrayList<>();
		records.add(errRow);

		client.operate(bp, records);

		// Before the fix the single-key write path opted out of verbosity and dropped the
		// detail, so this message came back null.
		assertEquals("Unexpected result code", ResultCode.BIN_TYPE_ERROR, errRow.resultCode);
		assertNotNull("Single-key write row lost its server message", errRow.serverMessage);
		assertTrue("Expected append-type detail, got: " + errRow.serverMessage,
			errRow.serverMessage.toLowerCase().contains("append"));
	}
}
