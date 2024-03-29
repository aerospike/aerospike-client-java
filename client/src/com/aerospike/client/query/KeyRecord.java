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
 * Container object for key identifier and record data.
 */
public final class KeyRecord {
	/**
	 * Unique identifier for record.
	 */
	public final Key key;

	/**
	 * Record header and bin data.
	 */
	public final Record record;

	/**
	 * Initialize key and record.
	 */
	public KeyRecord(Key key, Record record) {
		this.key = key;
		this.record = record;
	}

	/**
	 * Hash lookup uses key and record.
	 */
	@Override
	public int hashCode() {
		return Objects.hash(key, record);
	}

	/**
	 * Equality uses key and record.
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
	 * Convert key and record to string.
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
