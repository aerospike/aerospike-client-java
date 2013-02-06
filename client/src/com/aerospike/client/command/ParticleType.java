/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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
