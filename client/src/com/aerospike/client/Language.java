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
package com.aerospike.client;

/**
 * UDF language identifier; used when registering or executing user-defined functions.
 * <p>
 * Pass to {@link com.aerospike.client.AerospikeClient#register} for UDF package registration.
 *
 * <p><b>Example:</b>
 * <p>Register a UDF package using Language.LUA.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *   com.aerospike.client.task.RegisterTask task = client.register(null, "myudf.lua", "myudf.lua", Language.LUA);
 *   task.waitTillComplete();
 * } finally {
 *   client.close();
 * }
 * }</pre>
 *
 * @see com.aerospike.client.AerospikeClient#register
 */
public enum Language {
	/** Lua embedded programming language. */
	LUA;
}
