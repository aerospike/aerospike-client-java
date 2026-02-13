/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client.query;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

import java.util.Objects;

/**
 * Pairs a record key with its record data (bins, generation, expiration), as returned by query and scan.
 *
 * <p>Used by {@link com.aerospike.client.query.RecordSet} and query/scan listeners. The key may be {@code null}
 * for aggregation results that do not return keys; the record may be {@code null} when only digests are requested.
 *
 * <p>Iterate query/scan RecordSet and read key and record from each KeyRecord.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * RecordSet rs = client.query(policy, stmt);
 * try {
 *     for (KeyRecord kr : rs) {
 *         Key key = kr.key;
 *         Record rec = kr.record;
 *         if (rec != null) {
 *             Object val = rec.getValue("mybin");
 *         }
 *     }
 * } finally {
 *     rs.close();
 * }
 * }</pre>
 *
 * <p>Construct a KeyRecord pair from a key and record (e.g. for testing or custom iteration).</p>
 * <pre>{@code
 * Key key = new Key("ns", "set", "id");
 * Record record = client.get(null, key).record;
 * KeyRecord kr = new KeyRecord(key, record);
 * }</pre>
 *
 * @see com.aerospike.client.query.RecordSet
 * @see com.aerospike.client.Key
 * @see com.aerospike.client.Record
 */
public final class KeyRecord {
	/**
	 * The record key; may be {@code null} in some aggregation or digest-only results.
	 */
	public final Key key;

	/**
	 * The record (bins, generation, expiration); may be {@code null} if only digests were requested.
	 */
	public final Record record;

	/**
	 * Constructs a key-record pair.
	 *
	 * @param key the record key; may be {@code null}
	 * @param record the record data; may be {@code null}
	 */
	public KeyRecord(Key key, Record record) {
		this.key = key;
		this.record = record;
	}

	/**
	 * Returns a hash code based on key and record.
	 *
	 * @return the hash code
	 */
	@Override
	public int hashCode() {
		return Objects.hash(key, record);
	}

	/**
	 * Compares this key-record pair to the specified object for equality.
	 *
	 * @param obj the object to compare to
	 * @return {@code true} if equal, {@code false} otherwise
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof KeyRecord)) {
			return false;
		}
		KeyRecord that = (KeyRecord) obj;
		return Objects.equals(key, that.key) &&
				Objects.equals(record, that.record);
	}

	/**
	 * Returns a string representation of this key-record pair.
	 *
	 * @return string representation
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(1024);

		if (key != null) {
			sb.append(key.toString());
		}

		sb.append(':');

		if (record != null) {
			sb.append(record.toString());
		}
		return sb.toString();
	}
}
