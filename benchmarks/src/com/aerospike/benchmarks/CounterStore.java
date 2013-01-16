package com.aerospike.benchmarks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CounterStore {
	AtomicInteger failcnt            = new AtomicInteger();
	AtomicInteger tcounter           = new AtomicInteger();
	AtomicInteger rcounter           = new AtomicInteger();
	AtomicLong    rtdsum             = new AtomicLong();
	AtomicInteger wcounter           = new AtomicInteger();
	AtomicLong    wtdsum             = new AtomicLong();
	AtomicInteger timeElapsed        = new AtomicInteger();
	AtomicInteger sbrcounter         = new AtomicInteger();
	AtomicInteger sbwcounter         = new AtomicInteger();
	AtomicInteger loadValuesFinishedTasks = new AtomicInteger();
	AtomicBoolean loadValuesFinished = new AtomicBoolean(false);
	AtomicInteger valueMismatchCnt   = new AtomicInteger();
	AtomicInteger generationErrCnt   = new AtomicInteger();
	long start_time                  = 0;
}

