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

    //keep a windowed version of these stats for printing status
    private AtomicInteger windowoperations;
    private AtomicLong windowtotallatency;

    private AtomicLong min;
    private AtomicLong max;
    private String name;

	public LatencyManagerYcsb(String name) {
		this.name = name;
        _buckets = new AtomicInteger(1000);
        histogram = new AtomicLongArray(_buckets.get());
        histogramoverflow = new AtomicLong(0);
        operations = new AtomicInteger(0);
        totallatency = new AtomicLong(0);
        windowoperations = new AtomicInteger(0);
        windowtotallatency = new AtomicLong(0);
        min = new AtomicLong(-1);
        max = new AtomicLong(-1);
	}
	
	@Override
	public void add(long latency) {
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
		StringBuilder buffer = new StringBuilder(1024);
		double avgLatency = (((double) totallatency.get()) / ((double) operations.get()));
		buffer.append(name).append(":");
		buffer.append(" Ops:").append(operations.get());
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
        exporter.println(buffer.toString());
        windowoperations.set(0);
        windowtotallatency.set(0);
	}
}
