/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.client.operation;

/**
 * String operation policy write bit flags. Use BITWISE OR to combine flags. Example:
 *
 * <pre>{@code
 * int flags = StringWriteFlags.CREATE_ONLY | StringWriteFlags.NO_FAIL;
 * }</pre>
 */
public final class StringWriteFlags {
	/**
	 * Default. Allow create or update.
	 */
	public static final int DEFAULT = 0;

	/**
	 * Apply the operation only if the bin does not already exist. Against a live bin
	 * the server returns BIN_EXISTS_ERROR.
	 * <p>
	 * Valid only on the eight additive create-ops: insert, overwrite, concat, append,
	 * prepend, padStart, padEnd and repeat. On any other string modify op the server
	 * rejects it with PARAMETER_ERROR via that op's flag mask.
	 * <p>
	 * CREATE_ONLY combined with {@link #UPDATE_ONLY} is PARAMETER_ERROR, and CREATE_ONLY
	 * on a CTX (nested) path is PARAMETER_ERROR. None of those three rejections is
	 * suppressible by {@link #NO_FAIL}: the server raises them while parsing the
	 * operation's arguments, upstream of every NO_FAIL test.
	 */
	public static final int CREATE_ONLY = 1;

	/**
	 * Apply the operation only to an existing bin, disabling bin creation. On a missing
	 * bin the operation is a silent no-op and the bin is not created. Valid on all string
	 * modify ops.
	 * <p>
	 * Mutually exclusive with {@link #CREATE_ONLY}; combining the two is PARAMETER_ERROR.
	 */
	public static final int UPDATE_ONLY = 2;

	/**
	 * Do not raise an error when the modify itself cannot be applied. The operation
	 * becomes a silent success and the bin is left at its unmodified prior value.
	 * <p>
	 * NO_FAIL does not suppress every failure. A wrong bin type (BIN_TYPE_ERROR) and
	 * invalid UTF-8 in the bin (INVALID_ENCODING) surface regardless of the flag, as do
	 * the argument-parsing rejections listed on {@link #CREATE_ONLY}.
	 */
	public static final int NO_FAIL = 4;
}
