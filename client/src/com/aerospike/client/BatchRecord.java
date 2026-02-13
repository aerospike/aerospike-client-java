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
 * Holds the key and result (record or error) for one item in a batch operation.
 *
 * <p>After a batch call completes, each {@code BatchRecord} has a {@link #resultCode}; when it is
 * {@link ResultCode#OK}, {@link #record} is set. When an error occurred, {@link #record} is {@code null}
 * and {@link #inDoubt} indicates whether a write might have completed on the server.
 *
 * <p>Build a list of BatchRecords, call get, then check resultCode and record on each.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * List<BatchRecord> records = new ArrayList<>();
 * for (Key k : keys) {
 *     records.add(new BatchRecord(k, false));
 * }
 * client.get(null, records);
 * for (BatchRecord br : records) {
 *     if (br.resultCode == ResultCode.OK && br.record != null) {
 *         // use br.record
 *     } else {
 *         // handle br.resultCode, check br.inDoubt for writes
 *     }
 * }
 * }</pre>
 *
 * @see ResultCode
 * @see AerospikeException.BatchRecords
 * @see AerospikeException.BatchRecordArray
 */
public class BatchRecord {
	/**
	 * The key for this batch item; never {@code null}.
	 */
	public final Key key;

	/**
	 * The record returned for this key after the batch completes; {@code null} if the record was not found or an error occurred (see {@link #resultCode}).
	 */
	public Record record;

	/**
	 * Result code for this batch item (e.g. {@link ResultCode#OK}, {@link ResultCode#KEY_NOT_FOUND_ERROR}).
	 * When not {@link ResultCode#OK}, {@link #record} is typically {@code null}.
	 */
	public int resultCode;

	/**
	 * When {@code true}, a write may have completed on the server even though an error was reported (e.g. timeout after send).
	 */
	public boolean inDoubt;

	/**
	 * Whether this batch item includes a write operation; for internal use.
	 */
	public final boolean hasWrite;

	/**
	 * Constructs a batch record for the given key; result fields are reset for the upcoming batch call.
	 *
	 * @param key the key for this batch item; must not be {@code null}
	 * @param hasWrite whether this item performs a write
	 */
	public BatchRecord(Key key, boolean hasWrite) {
		this.key = key;
		this.resultCode = ResultCode.NO_RESPONSE;
		this.hasWrite = hasWrite;
	}

	/**
	 * Constructs a batch record with a successful read result.
	 *
	 * @param key the key; must not be {@code null}
	 * @param record the record returned; may be {@code null} if key not found
	 * @param hasWrite whether this item performs a write
	 */
	public BatchRecord(Key key, Record record, boolean hasWrite) {
		this.key = key;
		this.record = record;
		this.resultCode = ResultCode.OK;
		this.hasWrite = hasWrite;
	}

	/**
	 * Constructs a batch record with an error or partial result.
	 *
	 * @param key the key; must not be {@code null}
	 * @param record optional partial record; often {@code null} on error
	 * @param resultCode the result code (e.g. from {@link ResultCode})
	 * @param inDoubt whether a write may have completed on the server
	 * @param hasWrite whether this item performs a write
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
	 * Returns a string representation of this batch record (the key).
	 *
	 * @return string representation of the key
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
	 * Batch command type for classifying a batch record.
	 */
	public enum Type {
		/** Batch read operation. */
		BATCH_READ,
		/** Batch write operation. */
		BATCH_WRITE,
		/** Batch delete operation. */
		BATCH_DELETE,
		/** Batch UDF execute operation. */
		BATCH_UDF
	}
}
