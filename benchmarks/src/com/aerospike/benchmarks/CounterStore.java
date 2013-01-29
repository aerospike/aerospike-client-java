package com.aerospike.benchmarks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CounterStore {
	
	public static class Current {		
		AtomicInteger count = new AtomicInteger();
		AtomicInteger fail = new AtomicInteger();
	}
		
	Current write = new Current();
	Current read = new Current();
	
	long start_time = 0;

	AtomicInteger timeElapsed = new AtomicInteger();
	AtomicInteger tcounter = new AtomicInteger();
	AtomicInteger generationErrCnt = new AtomicInteger();
	AtomicInteger valueMismatchCnt = new AtomicInteger();
	AtomicInteger loadValuesFinishedTasks = new AtomicInteger();
	AtomicBoolean loadValuesFinished = new AtomicBoolean(false);
}
