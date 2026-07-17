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
 * Sort-metadata-only reduce spec produced by {@link Reduce#orderBy(String, BinDataType, Order, OrderByFlags)}.
 * <p>
 * Carries no combiner logic by itself. {@link Statement#setReduce(ReduceSpec...)} pairs this
 * with a {@link LimitReduceSpec} on the same bin and composes them into a {@link TopKReduceSpec}
 * via {@link TopKReduceSpec#compose(ReduceSpec, ReduceSpec)}. Not part of the public API surface
 * beyond the {@link Reduce#orderBy} factory method.
 */
final class OrderByReduceSpec implements ReduceSpec<Record, Record> {
	final String bin;
	final BinDataType type;
	final Order order;
	final OrderByFlags flags;

	OrderByReduceSpec(String bin, BinDataType type, Order order, OrderByFlags flags) {
		this.bin = bin;
		this.type = type;
		this.order = order;
		this.flags = flags;
	}

	@Override
	public void acceptPartial(Record record, Key key) {
		throw new UnsupportedOperationException("orderBy must be combined with limit (see Reduce.topK)");
	}

	@Override
	public Record getScalarResult() {
		throw new UnsupportedOperationException("orderBy must be combined with limit (see Reduce.topK)");
	}

	@Override
	public Record[] getResult() {
		throw new UnsupportedOperationException("orderBy must be combined with limit (see Reduce.topK)");
	}
}
