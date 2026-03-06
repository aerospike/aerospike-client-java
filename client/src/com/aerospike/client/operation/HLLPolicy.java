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
 * Policy for HyperLogLog operations; holds {@link HLLWriteFlags} used by {@link HLLOperation}.
 * <p>
 * Pass to {@link HLLOperation#init}, {@link HLLOperation#add}, and other HLLOperation methods.
 * <p>Use HLLPolicy with init and add.</p>
 * <pre>{@code
 * HLLPolicy policy = HLLPolicy.Default;
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.operate(null, key, HLLOperation.init(policy, "hll", 14));
 * client.operate(null, key, HLLOperation.add(policy, "hll", valueList));
 *
 * HLLPolicy createOnly = new HLLPolicy(HLLWriteFlags.CREATE_ONLY);
 * client.operate(null, key, HLLOperation.init(createOnly, "hll", 12));
 * }</pre>
 *
 * @see HLLOperation
 * @see HLLWriteFlags
 */
public final class HLLPolicy {
	/** Default policy with normal create/update semantics. */
	public static final HLLPolicy Default = new HLLPolicy();

	public final int flags;

	/**
	 * Use default {@link HLLWriteFlags} when performing {@link HLLOperation} operations.
	 */
	public HLLPolicy() {
		this(HLLWriteFlags.DEFAULT);
	}

	/**
	 * Use specified {@link HLLWriteFlags} when performing {@link HLLOperation} operations.
	 */
	public HLLPolicy(int flags) {
		this.flags = flags;
	}
}
