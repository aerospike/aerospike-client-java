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
 * Database operation definition.  The class is used in client's operate() method.
 */
public final class Operation {
	/**
	 * Create read bin database operation.
	 */
	public static Operation get(String binName) {
		return new Operation(Type.READ, binName);
	}

	/**
	 * Create read all record bins database operation.
	 */
	public static Operation get() {
		return new Operation(Type.READ);
	}

	/**
	 * Create read record header database operation.
	 */
	public static Operation getHeader() {
		return new Operation(Type.READ_HEADER);
	}

	/**
	 * Create set database operation.
	 */
	public static Operation put(Bin bin) {
		return new Operation(Type.WRITE, bin.name, bin.value);
	}

	/**
	 * Create string append database operation.
	 */
	public static Operation append(Bin bin) {
		return new Operation(Type.APPEND, bin.name, bin.value);
	}

	/**
	 * Create string prepend database operation.
	 */
	public static Operation prepend(Bin bin) {
		return new Operation(Type.PREPEND, bin.name, bin.value);
	}

	/**
	 * Create integer/double add database operation. If the record or bin does not exist, the
	 * record/bin will be created by default with the value to be added.
	 */
	public static Operation add(Bin bin) {
		return new Operation(Type.ADD, bin.name, bin.value);
	}

	/**
	 * Create touch record database operation.
	 */
	public static Operation touch() {
		return new Operation(Type.TOUCH);
	}

	/**
	 * Create delete record database operation.
	 */
	public static Operation delete() {
		return new Operation(Type.DELETE);
	}

	/**
	 * Create array of operations from varargs. This method can be useful when
	 * its important to save identical array pointer references. Using varargs
	 * directly always generates new references.
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
