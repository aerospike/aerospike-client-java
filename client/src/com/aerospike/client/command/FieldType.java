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

public final class FieldType {
	public static final int NAMESPACE = 0;
	public static final int TABLE = 1;
	public static final int KEY = 2;
	//public static final int BIN = 3;
	public static final int DIGEST_RIPE = 4;
	//public static final int GU_TID = 5;
	public static final int DIGEST_RIPE_ARRAY = 6;	
	public final static int TRAN_ID = 7;	// user supplied transaction id, which is simply passed back
	public final static int SCAN_OPTIONS = 8;
	public final static int INDEX_NAME = 21;
	public final static int INDEX_RANGE = 22;
	public final static int INDEX_FILTER = 23;
	public final static int INDEX_LIMIT = 24;
	public final static int INDEX_ORDER_BY = 25;
	public final static int UDF_PACKAGE_NAME = 30;
	public final static int UDF_FUNCTION = 31;
	public final static int UDF_ARGLIST = 32;
	public final static int UDF_OP = 33;
	public final static int QUERY_BINLIST = 40;
}
