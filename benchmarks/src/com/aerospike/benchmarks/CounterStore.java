/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.benchmarks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CounterStore {
	
	Current write = new Current();
	Current read = new Current();

	AtomicLong periodBegin = new AtomicLong();	
	AtomicInteger valueMismatchCnt = new AtomicInteger();
	AtomicInteger loadValuesFinishedTasks = new AtomicInteger();
	AtomicBoolean loadValuesFinished = new AtomicBoolean(false);

	public static class Current {
		AtomicInteger count = new AtomicInteger();
		AtomicInteger timeouts = new AtomicInteger();
		AtomicInteger errors = new AtomicInteger();
		LatencyManager latency;
	}	
}
