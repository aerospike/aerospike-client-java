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
 * Language for user-defined functions (UDFs) registered with the server.
 *
 * <p>Register a UDF with Language.LUA and wait for completion.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *     RegisterTask task = client.register(null, "myudf.lua", "myudf.lua", Language.LUA);
 *     task.waitTillComplete();
 * } finally { client.close(); }
 * }</pre>
 *
 * @see com.aerospike.client.AerospikeClient#register(com.aerospike.client.policy.Policy, String, String, Language)
 */
public enum Language {
	/** Lua; the supported UDF language for Aerospike. */
	LUA;
}
