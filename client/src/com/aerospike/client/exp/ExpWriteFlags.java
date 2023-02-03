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
package com.aerospike.client.exp;

/**
 * Expression write flags. 
 * To combine flags, use BITWISE OR. 
 * e.g. ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL
 */
public final class ExpWriteFlags {
	/**
	 * Default. Allow create or update.
	 */
	public static final int DEFAULT = 0;

	/**
	 * If bin does not exist, a new bin will be created.
	 * If bin exists, the operation will be denied.
	 * If bin exists, fail with {@link com.aerospike.client.ResultCode#BIN_EXISTS_ERROR}
	 * when {@link #POLICY_NO_FAIL} is not set.
	 */
	public static final int CREATE_ONLY = 1;

	/**
	 * If bin exists, the bin will be overwritten.
	 * If bin does not exist, the operation will be denied.
	 * If bin does not exist, fail with {@link com.aerospike.client.ResultCode#BIN_NOT_FOUND}
	 * when {@link #POLICY_NO_FAIL} is not set.
	 */
	public static final int UPDATE_ONLY = 2;

	/**
	 * If expression results in nil value, then delete the bin. Otherwise, fail with
	 * {@link com.aerospike.client.ResultCode#OP_NOT_APPLICABLE} when {@link #POLICY_NO_FAIL} is not set.
	 */
	public static final int ALLOW_DELETE = 4;

	/**
	 * Do not raise error if operation is denied.
	 */
	public static final int POLICY_NO_FAIL = 8;

	/**
	 * Ignore failures caused by the expression resolving to unknown or a non-bin type.
	 */
	public static final int EVAL_NO_FAIL = 16;
}
