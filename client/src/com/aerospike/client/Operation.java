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

/**
 * Defines a single read or write operation for use in {@link com.aerospike.client.AerospikeClient#operate}.
 *
 * <p>Use the static factory methods ({@link #get}, {@link #put}, {@link #add}, {@link #delete}, etc.) to build
 * operations; then pass an array of operations to operate(). Multiple operations in one call are applied atomically.
 *
 * <p>Use operate with add and get operations for atomic read-modify-write.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *     Key key = new Key("test", "set1", "id1");
 *     Record rec = client.operate(null, key,
 *         Operation.add(new Bin("count", 1)),
 *         Operation.get("count"));
 *     long count = (Long) rec.getValue("count");
 * } finally { client.close(); }
 * }</pre>
 *
 * @see com.aerospike.client.AerospikeClient#operate
 * @see Bin
 */
public final class Operation {
	/**
	 * Creates a read operation for the given bin.
	 *
	 * @param binName the bin name to read; must not be {@code null}
	 * @return a read operation for that bin
	 */
	public static Operation get(String binName) {
		return new Operation(Type.READ, binName);
	}

	/**
	 * Creates a read operation for all bins in the record.
	 *
	 * @return a read-all-bins operation
	 */
	public static Operation get() {
		return new Operation(Type.READ);
	}

	/**
	 * Creates a read operation for record metadata only (generation, expiration), no bins.
	 *
	 * @return a read-header operation
	 */
	public static Operation getHeader() {
		return new Operation(Type.READ_HEADER);
	}

	/**
	 * Creates a write operation that sets the given bin.
	 *
	 * @param bin the bin name and value; must not be {@code null}
	 * @return a write operation for that bin
	 */
	public static Operation put(Bin bin) {
		return new Operation(Type.WRITE, bin.name, bin.value);
	}

	/**
	 * Creates an append operation that appends the value to the string in the given bin.
	 *
	 * @param bin the bin name and value to append; must not be {@code null}
	 * @return an append operation
	 */
	public static Operation append(Bin bin) {
		return new Operation(Type.APPEND, bin.name, bin.value);
	}

	/**
	 * Creates a prepend operation that prepends the value to the string in the given bin.
	 *
	 * @param bin the bin name and value to prepend; must not be {@code null}
	 * @return a prepend operation
	 */
	public static Operation prepend(Bin bin) {
		return new Operation(Type.PREPEND, bin.name, bin.value);
	}

	/**
	 * Creates an add operation (integer or double). If the record or bin does not exist, it is created with the added value.
	 *
	 * @param bin the bin name and value to add; must not be {@code null}
	 * @return an add operation
	 */
	public static Operation add(Bin bin) {
		return new Operation(Type.ADD, bin.name, bin.value);
	}

	/**
	 * Creates a touch operation that updates the record's expiration (TTL) without reading or writing bins.
	 *
	 * @return a touch operation
	 */
	public static Operation touch() {
		return new Operation(Type.TOUCH);
	}

	/**
	 * Creates a delete operation that deletes the record.
	 *
	 * @return a delete operation
	 */
	public static Operation delete() {
		return new Operation(Type.DELETE);
	}

	/**
	 * Returns the given operations as an array (useful when you need a stable array reference).
	 *
	 * @param ops one or more operations
	 * @return the same operations as an array
	 */
	public static Operation[] array(Operation... ops) {
		return ops;
	}

	public static enum Type {
		READ(1, false),
		READ_HEADER(1, false),
		WRITE(2, true),
		CDT_READ(3, false),
		CDT_MODIFY(4, true),
		MAP_READ(3, false),
		MAP_MODIFY(4, true),
		ADD(5, true),
		EXP_READ(7, false),
		EXP_MODIFY(8, true),
		APPEND(9, true),
		PREPEND(10, true),
		TOUCH(11, true),
		BIT_READ(12, false),
		BIT_MODIFY(13, true),
		DELETE(14, true),
		HLL_READ(15, false),
		HLL_MODIFY(16, true);

		public final int protocolType;
		public final boolean isWrite;

		private Type(int protocolType, boolean isWrite) {
			this.protocolType = protocolType;
			this.isWrite = isWrite;
		}
	}

	/**
	 * Type of operation.
	 */
	public final Type type;

	/**
	 * Optional bin name used in operation.
	 */
	public final String binName;

	/**
	 * Optional argument to operation.
	 */
	public final Value value;

	public Operation(Type type, String binName, Value value) {
		this.type = type;
		this.binName = binName;
		this.value = value;
	}

	private Operation(Type type, String binName) {
		this.type = type;
		this.binName = binName;
		this.value = Value.getAsNull();
	}

	private Operation(Type type) {
		this.type = type;
		this.binName = null;
		this.value = Value.getAsNull();
	}
}
