/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
