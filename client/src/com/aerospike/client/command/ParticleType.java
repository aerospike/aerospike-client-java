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
	protected static final int NULL = 0;
	protected static final int INTEGER = 1;
	//protected static final int BIGNUM = 2;
	protected static final int STRING = 3;
	protected static final int BLOB = 4;
	//protected static final int TIMESTAMP = 5;
	//protected static final int DIGEST = 6;
	protected static final int JBLOB = 7;
	//protected static final int CSHARP_BLOB = 8;
	//protected static final int PYTHON_BLOB = 9;
	//protected static final int RUBY_BLOB = 10;
	//protected static final int PHP_BLOB = 11;
	//protected static final int ERLANG_BLOB = 12;
	//protected static final int SEGMENT_POINTER = 13;
	protected static final int LIST = 14;
	protected static final int DICT = 15;
}
