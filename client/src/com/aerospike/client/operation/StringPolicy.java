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
 * String operation policy.
 * <p>
 * This is a per-operation policy carrying {@link StringWriteFlags}. It is
 * passed inline to each {@link StringOperation} builder method and is
 * <b>not</b> part of the client's dynamic configuration: there is no
 * {@code stringPolicyDefault} on {@link com.aerospike.client.policy.ClientPolicy}
 * and no corresponding stanza in the YAML config schema. Changing the flags
 * requires constructing a new {@code StringPolicy} and passing it to the
 * operation, not editing a config file at runtime. This mirrors how
 * {@code BitPolicy} and {@code HLLPolicy} are scoped.
 */
public final class StringPolicy {
	/**
	 * Default string bin write semantics.
	 */
	public static final StringPolicy Default = new StringPolicy();

	public final int flags;

	/**
	 * Use default {@link StringWriteFlags} when performing {@link StringOperation} modify operations.
	 */
	public StringPolicy() {
		this(StringWriteFlags.DEFAULT);
	}

	/**
	 * Use specified {@link StringWriteFlags} when performing {@link StringOperation} modify operations.
	 */
	public StringPolicy(int flags) {
		this.flags = flags;
	}
}
