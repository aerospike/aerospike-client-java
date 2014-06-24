/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.benchmarks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CounterStore {
	
	Current write = new Current();
	Current read = new Current();
	
	AtomicLong periodBegin = new AtomicLong();	
	AtomicInteger readNotFound = new AtomicInteger();
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
