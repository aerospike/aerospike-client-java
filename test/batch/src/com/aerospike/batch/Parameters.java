/*
 *  Copyright 2012 by Aerospike, Inc.  All rights reserved.
 *  
 *  Availability of this source code to partners and customers includes
 *  redistribution rights covered by individual contract. Please check
 *  your contract for exact rights and responsibilities.
 */
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
