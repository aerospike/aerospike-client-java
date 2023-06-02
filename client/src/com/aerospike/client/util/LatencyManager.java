/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.client.util;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.cluster.Node;

public final class LatencyManager {
    private static final DecimalFormat PercentFormat = new DecimalFormat("0.00");

    private final Bucket[] buckets;
	private final int latencyShift;

	public LatencyManager(int latencyColumns, int latencyShift) {
		this.latencyShift = latencyShift;
		buckets = new Bucket[latencyColumns];

		for (int i = 0; i < buckets.length; i++) {
			buckets[i] = new Bucket();
		}
	}

	public void add(double elapsed) {
		int index = getIndex(elapsed);
		buckets[index].increment();
	}

	private int getIndex(double elapsed) {
		int e = (int)Math.ceil(elapsed);
		int lastBucket = buckets.length - 1;
		int limit = 1;

		for (int i = 0; i < lastBucket; i++) {
			if (e <= limit) {
				return i;
			}
			limit <<= latencyShift;
		}
		return lastBucket;
	}

	public static void printHeader(StringBuilder sb, int latencyColumns, int latencyShift) {
		sb.append(" latency <=1ms >1ms");

		int limit = 1;

		for (int i = 2; i < latencyColumns; i++) {
			limit <<= latencyShift;
			String s = " >" + limit + "ms";
			sb.append(s);
		}
	}

	/**
	 * Print latency percents for specified cumulative ranges.
	 * This function is not absolutely accurate for a given time slice because this method
	 * is not synchronized with the Add() method.  Some values will slip into the next iteration.
	 * It is not a good idea to add extra locks just to measure performance since that actually
	 * affects performance.  Fortunately, the values will even out over time (ie. no double counting).
	 */
	public boolean printResults(Node node, StringBuilder sb, String type) {
		// Capture snapshot and make buckets cumulative.
		int[] array = new int[buckets.length];
		int sum = 0;
		int count;

		for (int i = buckets.length - 1; i >= 1; i--) {
			count = buckets[i].reset();
			array[i] = count + sum;
			sum += count;
		}

		// The first bucket (<=1ms) does not need a cumulative adjustment.
		count = buckets[0].reset();
		array[0] = count;
		sum += count;

		if (sum == 0) {
			// Skip over results that do not contain data.
			return false;
		}

		// Print cumulative results.
		sb.setLength(0);
		sb.append(' ');
		sb.append(type);

		double sumDouble = (double)sum;
		int limit = 1;

		printColumn(sb, limit, sumDouble, array[0]);
		printColumn(sb, limit, sumDouble, array[1]);

		for (int i = 2; i < array.length; i++) {
			limit <<= latencyShift;
			printColumn(sb, limit, sumDouble, array[i]);
		}
		return true;
	}

	public String printSummary(StringBuilder sb, String prefix) {
		int[] array = new int[buckets.length];
		int sum = 0;
		int count;

		for (int i = buckets.length - 1; i >= 1; i--) {
			count = buckets[i].sum;
			array[i] = count + sum;
			sum += count;
		}

		// The first bucket (<=1ms) does not need a cumulative adjustment.
		count = buckets[0].sum;
		array[0] = count;
		sum += count;

		// Print cumulative results.
		sb.setLength(0);
		sb.append(prefix);
		int spaces = 6 - prefix.length();

		for (int j = 0; j < spaces; j++) {
			sb.append(' ');
		}

		double sumDouble = (double)sum;
		int limit = 1;

		printColumn(sb, limit, sumDouble, array[0]);
		printColumn(sb, limit, sumDouble, array[1]);

		for (int i = 2; i < array.length; i++) {
			limit <<= latencyShift;
			printColumn(sb, limit, sumDouble, array[i]);
		}
		return sb.toString();
	}

	private void printColumn(StringBuilder sb, int limit, double sum, int count) {
		sb.append(' ');
		double percent = (count > 0) ? (double)count * 100.0 / sum : 0.0;
		sb.append(PercentFormat.format(percent));
		sb.append(":");
		sb.append(count);
	}

	private static final class Bucket {
		AtomicInteger count = new AtomicInteger();
		public int sum = 0;

		public void increment() {
			count.getAndIncrement();
		}

		public int reset() {
			int c = count.getAndSet(0);
		    sum += c;
		    return c;
		}
	}
}
