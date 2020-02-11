/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class LatencyManagerYcsb implements LatencyManager {
    public static final String BUCKETS = "histogram.buckets";
    public static final String BUCKETS_DEFAULT = "1000";

    private AtomicInteger _buckets;
    private AtomicLongArray histogram;
    private AtomicLong histogramoverflow;
    private AtomicInteger operations;
    private AtomicLong totallatency;
    private AtomicInteger warmupCount;
    private volatile boolean warmupComplete = false;

    //keep a windowed version of these stats for printing status
    private AtomicInteger windowoperations;
    private AtomicLong windowtotallatency;

    private AtomicLong min;
    private AtomicLong max;
    private String name;

	public LatencyManagerYcsb(String name, int warmupCount) {
		this.name = name;
        _buckets = new AtomicInteger(1000);
        histogram = new AtomicLongArray(_buckets.get());
        histogramoverflow = new AtomicLong(0);
        operations = new AtomicInteger(0);
        totallatency = new AtomicLong(0);
        windowoperations = new AtomicInteger(0);
        windowtotallatency = new AtomicLong(0);
        this.warmupCount = new AtomicInteger(warmupCount);
        warmupComplete = warmupCount <= 0;
        min = new AtomicLong(-1);
        max = new AtomicLong(-1);
	}

	@Override
	public void add(long latency) {
		if (!warmupComplete) {
			return;
		}

		// Latency is specified in ns
		long latencyUs = latency / 1000;
		long latencyMs = latencyUs / 1000;
        if (latencyMs >= _buckets.get()) {
            histogramoverflow.incrementAndGet();
        } else {
            histogram.incrementAndGet((int)latencyMs);
        }
        operations.incrementAndGet();
        totallatency.addAndGet(latencyUs);
        windowoperations.incrementAndGet();
        windowtotallatency.addAndGet(latencyUs);

        if ((min.get() < 0) || (latencyUs < min.get())) {
            min.set(latencyUs);
        }

        if ((max.get() < 0) || (latencyUs > max.get())) {
            max.set(latencyUs);
        }
	}

	@Override
	public void printHeader(PrintStream stream) {
	}

	@Override
	public void printResults(PrintStream exporter, String prefix) {
		if (!warmupComplete) {
			int countRemaining = warmupCount.decrementAndGet();
			if (countRemaining <= 0) {
				warmupComplete = true;
			}
			exporter.println("Warming up (" + countRemaining + " left)...");
			return;
		}
		StringBuilder buffer = new StringBuilder(1024);
		double avgLatency = (((double) totallatency.get()) / ((double) operations.get()));
		double windowAvgLatency = (((double) windowtotallatency.get()) / ((double) windowoperations.get()));
		buffer.append(name).append(": Period[");
		buffer.append("Ops:").append(windowoperations.get());
		buffer.append(" Avg Latency:").append((long)windowAvgLatency).append("us");

		buffer.append("] Total[Ops:").append(operations.get());
		buffer.append(" Latency:(avg:").append((long)avgLatency).append("us");
		buffer.append(" Min:").append(min.get()).append("us");
		buffer.append(" Max:").append(max.get()).append("us)");

        int opcounter = 0;
        boolean done95th = false;
        for (int i = 0; i < _buckets.get(); i++) {
            opcounter += histogram.get(i);
            double percentage = ((double) opcounter) / ((double) operations.get());
            if ((!done95th) && percentage >= 0.95) {
        		buffer.append(" 95th% Latency:").append(i).append("ms");
                done95th = true;
            }
            if (percentage >= 0.99) {
        		buffer.append(" 99th% Latency:").append(i).append("ms");
                break;
            }
        }
        buffer.append(']');
        exporter.println(buffer.toString());
        windowoperations.set(0);
        windowtotallatency.set(0);
	}

	@Override
	public void printSummary(PrintStream stream, String prefix) {
	}

	@Override
	public void printSummaryHeader(PrintStream stream) {
	}
}
