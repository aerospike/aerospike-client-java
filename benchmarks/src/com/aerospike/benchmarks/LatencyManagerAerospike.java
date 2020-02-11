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

public class LatencyManagerAerospike implements LatencyManager {
	private static final long NS_TO_MS = 1000000;
	private static final long NS_TO_US = 1000;

	private final Bucket[] buckets;
	private final int lastBucket;
	private final int bitShift;
	private final boolean showMicroSeconds;
	private String header;

	public LatencyManagerAerospike(int columns, int bitShift, boolean showMicroSeconds) {
		this.lastBucket = columns - 1;
		this.bitShift = bitShift;
		this.showMicroSeconds = showMicroSeconds;
		buckets = new Bucket[columns];

		for (int i = 0; i < columns; i++) {
			buckets[i] = new Bucket();
		}
		formHeader();
	}

	private void formHeader() {
		int limit = 1;
		String units = showMicroSeconds ? "us" : "ms";
		StringBuilder s = new StringBuilder(64);
		s.append("      <=1").append(units).append(" >1").append(units);

		for (int i = 2; i < buckets.length; i++) {
			limit <<= bitShift;
			s.append(" >").append(limit).append(units);
		}
		header = s.toString();
	}

	public void add(long elapsed) {
		int index = getIndex(elapsed);
		buckets[index].count.incrementAndGet();
	}

	private int getIndex(long elapsedNanos) {
		long limit = 1L;
		long elapsed;

		if (showMicroSeconds) {
			elapsed = elapsedNanos / NS_TO_US;

			// Round up elapsed to nearest microsecond.
			if ((elapsedNanos - (elapsed * NS_TO_US)) > 0) {
				elapsed++;
			}
		}
		else {
			elapsed = elapsedNanos / NS_TO_MS;

			// Round up elapsed to nearest millisecond.
			if ((elapsedNanos - (elapsed * NS_TO_MS)) > 0) {
				elapsed++;
			}
		}

		for (int i = 0; i < lastBucket; i++) {
			if (elapsed <= limit) {
				return i;
			}
			limit <<= bitShift;
		}
		return lastBucket;
	}

	public void printHeader(PrintStream stream) {
		stream.println(header);
	}

	/**
	 * Print latency percents for specified cumulative ranges.
	 * This function is not absolutely accurate for a given time slice because this method
	 * is not synchronized with the add() method.  Some values will slip into the next iteration.
	 * It is not a good idea to add extra locks just to measure performance since that actually
	 * affects performance.  Fortunately, the values will even out over time
	 * (ie. no double counting).
	 */
	public void printResults(PrintStream stream, String prefix) {
		// Capture snapshot and make buckets cumulative.
		int[] array = new int[buckets.length];
		int sum = 0;
		int count;

		for (int i = buckets.length - 1; i >= 1 ; i--) {
			 count = buckets[i].reset();
			 array[i] = count + sum;
			 sum += count;
		}
		// The first bucket (<=1ms) does not need a cumulative adjustment.
		count = buckets[0].reset();
		array[0] = count;
		sum += count;

		// Print cumulative results.
		stream.print(prefix);
	//		stream.print(' ' );
	//		stream.print(sum);
	    int spaces = 6 - prefix.length();

	    for (int j = 0; j < spaces; j++) {
	    	stream.print(' ');
	    }

	    double sumDouble = (double)sum;
	    int limit = 1;

	    printColumn(stream, limit, sumDouble, array[0]);
	    printColumn(stream, limit, sumDouble, array[1]);

	    for (int i = 2; i < array.length; i++) {
	        limit <<= bitShift;
	        printColumn(stream, limit, sumDouble, array[i]);
	    }
		stream.println();
	}

	public void printSummaryHeader(PrintStream stream) {
		stream.println("Latency Summary");
		stream.println(header);
	}

	public void printSummary(PrintStream stream, String prefix) {
		int[] array = new int[buckets.length];
		int sum = 0;
		int count;

		for (int i = buckets.length - 1; i >= 1 ; i--) {
			count = buckets[i].sum;
			array[i] = count + sum;
			sum += count;
		}
		// The first bucket (<=1ms) does not need a cumulative adjustment.
		count = buckets[0].sum;
		array[0] = count;
		sum += count;

		// Print cumulative results.
		stream.print(prefix);
	    int spaces = 6 - prefix.length();

	    for (int j = 0; j < spaces; j++) {
	    	stream.print(' ');
	    }

	    double sumDouble = (double)sum;
	    int limit = 1;

	    printColumn(stream, limit, sumDouble, array[0]);
	    printColumn(stream, limit, sumDouble, array[1]);

	    for (int i = 2; i < array.length; i++) {
	        limit <<= bitShift;
	        printColumn(stream, limit, sumDouble, array[i]);
	    }
		stream.println();
	}

	private void printColumn(PrintStream stream, int limit, double sum, int value) {
	    long percent = 0;

	    if (value > 0) {
	        percent = Math.round((double)value * 100.0 / sum);
	    }
	    String percentString = Long.toString(percent) + "%";
	    int spaces = Integer.toString(limit).length() + 4 - percentString.length();

	    for (int j = 0; j < spaces; j++) {
	    	stream.print(' ');
	    }
	    stream.print(percentString);
	}

	private static final class Bucket {
		final AtomicInteger count = new AtomicInteger();
		int sum = 0;

		private int reset() {
			int c = count.getAndSet(0);
			sum += c;
			return c;
		}
	}
}
