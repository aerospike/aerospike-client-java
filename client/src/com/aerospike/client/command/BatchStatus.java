/*
 * Copyright 2012-2022 Aerospike, Inc.
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
package com.aerospike.client.command;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;

public final class BatchStatus implements BatchNodeList.IBatchStatus {
	private RuntimeException exception;
	private boolean error;
	private final boolean hasResultCode;

	public BatchStatus(boolean hasResultCode) {
		this.hasResultCode = hasResultCode;
	}

	@Override
	public void batchKeyError(Key key, int index, AerospikeException ae, boolean inDoubt, boolean hasWrite) {
		// Only used in async commands with a sequence listener.
	}

	@Override
	public void batchKeyError(AerospikeException e) {
		error = true;

		if (! hasResultCode) {
			// Legacy batch read commands that do not store a key specific resultCode.
			// Store exception and throw on batch completion.
			if (exception == null) {
				exception = e;
			}
		}
	}

	public void setRowError() {
		// Indicate that a key specific error occurred.
		error = true;
	}

	public boolean getStatus() {
		return !error;
	}

	public void setException(RuntimeException e) {
		error = true;

		if (exception == null) {
			exception = e;
		}
	}

	public void checkException() {
		if (exception != null) {
			throw exception;
		}
	}
}
