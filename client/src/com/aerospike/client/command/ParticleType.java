/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
package com.aerospike.client.command;

public final class ParticleType {
	// Server particle types. Unsupported types are commented out.
	public static final int NULL = 0;
	public static final int INTEGER = 1;
	//public static final int BIGNUM = 2;
	public static final int STRING = 3;
	public static final int BLOB = 4;
	//public static final int TIMESTAMP = 5;
	//public static final int DIGEST = 6;
	public static final int JBLOB = 7;
	//public static final int CSHARP_BLOB = 8;
	//public static final int PYTHON_BLOB = 9;
	//public static final int RUBY_BLOB = 10;
	//public static final int PHP_BLOB = 11;
	//public static final int ERLANG_BLOB = 12;
	//public static final int SEGMENT_POINTER = 13;
	//public static final int RTA_LIST = 14;
	//public static final int RTA_DICT = 15;
	//public static final int RTA_APPEND_DICT = 16;
	//public static final int RTA_APPEND_LIST = 17;
	//public static final int LUA_BLOB = 18;
	public static final int MAP = 19;
	public static final int LIST = 20;
}
