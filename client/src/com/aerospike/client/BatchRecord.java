/*
 * Copyright 2012-2024 Aerospike, Inc.
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

import com.aerospike.client.configuration.*;
import com.aerospike.client.policy.Policy;

/**
 * Batch key and record result.
 */
public class BatchRecord {
	/**
	 * Key.
	 */
	public final Key key;

	/**
	 * Record result after batch command has completed.  Will be null if record was not found
	 * or an error occurred. See {@link BatchRecord#resultCode}.
	 */
	public Record record;

	/**
	 * Result code for this returned record. See {@link com.aerospike.client.ResultCode}.
	 * If not {@link com.aerospike.client.ResultCode#OK}, the record will be null.
	 */
	public int resultCode;

	/**
	 * Is it possible that the write command may have completed even though an error
	 * occurred for this record. This may be the case when a client error occurs (like timeout)
	 * after the command was sent to the server.
	 */
	public boolean inDoubt;

	/**
	 * Does this command contain a write operation. For internal use only.
	 */
	public final boolean hasWrite;

	/**
	 * Initialize batch key.
	 */
	public BatchRecord(Key key, boolean hasWrite) {
		this.key = key;
		this.resultCode = ResultCode.NO_RESPONSE;
		this.hasWrite = hasWrite;
	}

	/**
	 * Initialize batch key and record.
	 */
	public BatchRecord(Key key, Record record, boolean hasWrite) {
		this.key = key;
		this.record = record;
		this.resultCode = ResultCode.OK;
		this.hasWrite = hasWrite;
	}

	/**
	 * Error constructor.
	 */
	public BatchRecord(Key key, Record record, int resultCode, boolean inDoubt, boolean hasWrite) {
		this.key = key;
		this.record = record;
		this.resultCode = resultCode;
		this.inDoubt = inDoubt;
		this.hasWrite = hasWrite;
	}

	/**
	 * Prepare for upcoming batch call. Reset result fields because this instance might be
	 * reused. For internal use only.
	 */
	public final void prepare() {
		this.record = null;
		this.resultCode = ResultCode.NO_RESPONSE;
		this.inDoubt = false;
	}

	/**
	 * Set record result. For internal use only.
	 */
	public final void setRecord(Record record) {
		this.record = record;
		this.resultCode = ResultCode.OK;
	}

	/**
	 * Set error result. For internal use only.
	 */
	public final void setError(int resultCode, boolean inDoubt) {
		this.resultCode = resultCode;
		this.inDoubt = inDoubt;
	}

	/**
	 * Convert to string.
	 */
	@Override
	public String toString() {
		return key.toString();
	}

	/**
	 * Return batch command type. For internal use only.
	 */
	public Type getType() {
		return null;
	}

	/**
	 * Optimized reference equality check to determine batch wire protocol repeat flag.
	 * For internal use only.
	 */
	public boolean equals(BatchRecord other, ConfigurationProvider configProvider) {
		return false;
	}

	/**
	 * Return wire protocol size. For internal use only.
	 */
	public int size(Policy parentPolicy, ConfigurationProvider configProvider) {
		return 0;
	}

	/**
	 * Batch command type.
	 */
	public enum Type {
		BATCH_READ,
		BATCH_WRITE,
		BATCH_DELETE,
		BATCH_UDF
	}
}
