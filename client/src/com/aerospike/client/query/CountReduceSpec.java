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
package com.aerospike.client.query;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Scalar COUNT reduce combiner. Backs {@link Reduce#count()}.
 * <p>
 * Counts one per {@link #acceptPartial(Record, Key)} call (one per matching record). Not part
 * of the public API surface beyond the {@link Reduce#count} factory method.
 */
final class CountReduceSpec implements ReduceSpec<Record, Long> {
	private long count;

	@Override
	public void acceptPartial(Record record, Key key) {
		count++;
	}

	@Override
	public Long getScalarResult() {
		return count;
	}

	@Override
	public Long[] getResult() {
		return new Long[] { getScalarResult() };
	}
}
