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

/**
 * Result of a scalar reduce query. Returned by
 * {@link com.aerospike.client.AerospikeClient#queryReduce(com.aerospike.client.policy.QueryPolicy, Statement)}
 * for statements with a scalar reduce spec set via
 * {@link Statement#setReduce(ReduceSpec...)} ({@link Reduce#sum}, {@link Reduce#count},
 * {@link Reduce#min}, or {@link Reduce#max}).
 */
public final class ReduceResult {
	private final Object value;

	public ReduceResult(Object value) {
		this.value = value;
	}

	/**
	 * Return the raw scalar value merged across all nodes.
	 */
	public Object getObject() {
		return value;
	}

	/**
	 * Return the scalar value as a {@link Number}. Valid for all scalar reduces
	 * ({@link Reduce#sum}, {@link Reduce#count}, {@link Reduce#min}, {@link Reduce#max}).
	 */
	public Number getNumber() {
		return (Number)value;
	}

	/**
	 * Return the scalar value as a {@code long}. Valid for {@link Reduce#sum} and
	 * {@link Reduce#count}, which always return {@link Long}.
	 */
	public long getLong() {
		return ((Number)value).longValue();
	}

	/**
	 * Return the scalar value as a {@code double}. Valid for {@link Reduce#min} and
	 * {@link Reduce#max} on {@link BinDataType#DOUBLE} bins.
	 */
	public double getDouble() {
		return ((Number)value).doubleValue();
	}

	@Override
	public String toString() {
		return String.valueOf(value);
	}
}
