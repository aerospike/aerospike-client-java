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
 * Live-server verification that the extended error detail (subcode / message) is
 * surfaced per row on batch commands, mirroring the single-record coverage in
 * {@link TestErrorDetailVerbosity} / {@link TestErrorDetailSubcode}.
 *
 * <p>The detail is batch-wide opt-in: the parent {@link BatchPolicy#errorDetailVerbosity}
 * is folded into each row's info4 verbosity bits, and the server attaches a per-row
 * error detail (field 45) that the client decodes onto {@link BatchRecord#serverMessage},
 * {@link BatchRecord#subcode}, and {@link BatchRecord#expTrace}. Requires an 8.1.3+ server.
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
	 * With verbosity opted in, an erroring batch row surfaces the server subcode and
	 * message, while a successful row in the same batch carries no detail.
	 */
	@Test
	public void testBatchRowSurfacesSubcode() {
		BatchPolicy bp = new BatchPolicy(client.getBatchParentPolicyWriteDefault());
		bp.errorDetailVerbosity = 2;

		// Error row: list get with an out-of-bounds index -> OP_NOT_APPLICABLE + CDT subcode.
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

		// Error row carries the server subcode and formatted message.
		assertEquals("Unexpected result code", ResultCode.OP_NOT_APPLICABLE, errRow.resultCode);
		assertEquals("Unexpected subcode", SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, errRow.subcode);
		assertNotNull("Expected server error message", errRow.serverMessage);
		assertTrue("Expected 'subcode=" + SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS + "' in: " + errRow.serverMessage,
			errRow.serverMessage.contains("subcode=" + SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS));

		// Success row carries no error detail.
		assertEquals(ResultCode.OK, okRow.resultCode);
		assertNull("OK row should have no server message", okRow.serverMessage);
		assertEquals("OK row should have no subcode", SubCode.NONE, okRow.subcode);
		assertNull("OK row should have no expression trace", okRow.expTrace);
	}

	/**
	 * Without opting in (default verbosity 0), the row still reports the result code but
	 * the server attaches no extended detail, so subcode/message stay cleared.
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
		assertEquals("No detail expected at verbosity 0", SubCode.NONE, errRow.subcode);
		assertNull("No detail expected at verbosity 0", errRow.serverMessage);
		assertNull("No detail expected at verbosity 0", errRow.expTrace);
	}
}
