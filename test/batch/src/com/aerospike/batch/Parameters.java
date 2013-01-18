package com.aerospike.batch;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

public final class Parameters {
	String  host;
	int		port;
	String  namespace;
	String  set;
	String  bin;
	boolean insertData;
	int     startKey;
	int     numKeys;
	int     keyLength;
	int     numIterations;
	int     timeout;
	boolean verbose;
	Key[] keys;
	AerospikeClient client;	
}
