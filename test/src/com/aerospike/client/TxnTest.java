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
package com.aerospike.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.aerospike.client.AerospikeException;

public class TxnTest {
	@Test
	public void markCommitFailedFromOpen() {
		Txn txn = new Txn();
		txn.markCommitFailed();
		assertEquals(Txn.State.COMMIT_FAILED, txn.getState());
	}

	@Test
	public void markCommitFailedFromVerified() {
		Txn txn = new Txn();
		txn.setState(Txn.State.VERIFIED);
		txn.markCommitFailed();
		assertEquals(Txn.State.COMMIT_FAILED, txn.getState());
	}

	@Test
	public void markCommitFailedPreservesAborted() {
		Txn txn = new Txn();
		txn.setState(Txn.State.ABORTED);
		txn.markCommitFailed();
		assertEquals(Txn.State.ABORTED, txn.getState());
	}

	@Test
	public void markCommitFailedPreservesCommitted() {
		Txn txn = new Txn();
		txn.setState(Txn.State.COMMITTED);
		txn.markCommitFailed();
		assertEquals(Txn.State.COMMITTED, txn.getState());
	}

	@Test
	public void verifyCommandRejectsCommitFailed() {
		Txn txn = new Txn();
		txn.setState(Txn.State.COMMIT_FAILED);

		try {
			txn.verifyCommand();
			fail("Expected AerospikeException for COMMIT_FAILED state");
		}
		catch (AerospikeException ex) {
			assertEquals(ResultCode.TXN_FAILED, ex.getResultCode());
		}
	}
}
