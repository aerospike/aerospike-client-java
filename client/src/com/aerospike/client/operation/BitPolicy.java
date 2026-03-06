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
 * Policy for bit (byte[]) operations; holds {@link BitWriteFlags} used by {@link BitOperation}.
 * <p>
 * Pass to {@link BitOperation#resize}, {@link BitOperation#insert}, {@link BitOperation#set}, and other BitOperation methods.
 * <p>Use BitPolicy with resize and set.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * BitPolicy policy = BitPolicy.Default;
 * client.operate(null, key, BitOperation.resize(policy, "bits", 8, BitResizeFlags.DEFAULT));
 *
 * BitPolicy createOnly = new BitPolicy(BitWriteFlags.CREATE_ONLY);
 * client.operate(null, key, BitOperation.set(createOnly, "bits", 0, new byte[] { 1, 2, 3 }));
 * }</pre>
 *
 * @see BitOperation
 * @see BitWriteFlags
 */
public final class BitPolicy {
	/** Default policy with normal create/update semantics. */
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
