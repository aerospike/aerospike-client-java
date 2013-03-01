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
	public final static int UDF_FILENAME = 30;
	public final static int UDF_FUNCTION = 31;
	public final static int UDF_ARGLIST = 32;
	public final static int QUERY_BINLIST = 40;
}
