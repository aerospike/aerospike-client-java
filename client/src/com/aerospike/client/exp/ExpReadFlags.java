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
 * Flags for {@link ExpOperation#read}; control behavior when expression evaluation fails (e.g. missing bin).
 * <p>
 * Use with {@link ExpOperation#read}. {@link #EVAL_NO_FAIL} returns the record without the expression result instead of failing.
 * <p>Use ExpReadFlags with ExpOperation.read.</p>
 * <pre>{@code
 * Record rec = client.operate(null, key, ExpOperation.read("EV", exp, ExpReadFlags.DEFAULT));
 * Record rec2 = client.operate(null, key, ExpOperation.read("EV", exp, ExpReadFlags.EVAL_NO_FAIL));
 * }</pre>
 *
 * @see ExpOperation#read
 */
public final class ExpReadFlags {
	/** Default: fail if expression cannot be evaluated (e.g. bin missing). */
	public static final int DEFAULT = 0;

	/** Ignore failures when expression resolves to unknown or non-bin type; record still returned. */
	public static final int EVAL_NO_FAIL = 16;
}
