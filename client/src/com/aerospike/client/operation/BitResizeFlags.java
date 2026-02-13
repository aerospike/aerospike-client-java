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
package com.aerospike.client.operation;

/**
 * Bitwise operation flags for resize. Use with {@link BitOperation#resize(BitPolicy, String, int, int)};
 * combine with BITWISE OR if needed.
 * <p>Use DEFAULT, FROM_FRONT, GROW_ONLY, or SHRINK_ONLY when resizing bit bins.</p>
 * <pre>{@code
 * client.operate(null, key, BitOperation.resize(BitPolicy.Default, "bits", 8, BitResizeFlags.DEFAULT));
 * client.operate(null, key, BitOperation.resize(BitPolicy.Default, "bits", 16, BitResizeFlags.GROW_ONLY));
 * }</pre>
 */
public final class BitResizeFlags {
	/**
	 * Default.
	 */
	public static final int DEFAULT = 0;

	/**
	 * Add/remove bytes from the beginning instead of the end.
	 */
	public static final int FROM_FRONT = 1;

	/**
	 * Only allow the byte[] size to increase.
	 */
	public static final int GROW_ONLY = 2;

	/**
	 * Only allow the byte[] size to decrease.
	 */
	public static final int SHRINK_ONLY = 4;
}
