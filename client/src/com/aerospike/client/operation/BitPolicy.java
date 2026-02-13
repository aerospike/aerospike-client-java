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
 * Bit operation policy. Use with {@link BitOperation} (resize, insert, set, add, etc.); pass
 * {@link #Default} or construct with {@link BitWriteFlags}.
 * <p>Use default policy or custom flags when performing bit operations.</p>
 * <pre>{@code
 * BitPolicy policy = BitPolicy.Default;
 * client.operate(null, key, BitOperation.resize(policy, "bits", 8, BitResizeFlags.DEFAULT));
 * BitPolicy custom = new BitPolicy(BitWriteFlags.CREATE_ONLY | BitWriteFlags.NO_FAIL);
 * }</pre>
 */
public final class BitPolicy {
	/**
	 * Default byte[] with normal bin write semantics.
	 */
	public static final BitPolicy Default = new BitPolicy();

	public final int flags;

	/**
	 * Use default {@link BitWriteFlags} when performing {@link BitOperation} operations.
	 */
	public BitPolicy() {
		this(BitWriteFlags.DEFAULT);
	}

	/**
	 * Use specified {@link BitWriteFlags} when performing {@link BitOperation} operations.
	 */
	public BitPolicy(int flags) {
		this.flags = flags;
	}
}
