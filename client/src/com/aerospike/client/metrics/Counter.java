/*
 * Copyright (c) 2012-2025 Aerospike, Inc.
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

package com.aerospike.client.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

public class Counter {
	private final ConcurrentHashMap<String, AtomicLong> counterMap = new ConcurrentHashMap<>();
	private final static String noNSLabel = "";

	/**
	 * A Counter is a container for a namespace-aggregate map of AtomicLong counters
	 */
	public Counter() {
	}

	/**
	 * Increment the counter by 1 for the given namespace
	 *
	 * @param ns - the namespace for the counter
	 */
	public void increment(String ns) {
		String namespace = (ns == null) ? noNSLabel : ns;
		counterMap.compute(namespace, (k, v) -> {
			if (v == null) {
				return new AtomicLong(1);
			} else {
				v.incrementAndGet();
				return v;
			}
		});
	}

	/**
	 * Increment the counter by the provided count amount for the given namespace
	 *
	 * @param ns    - the namespace for the counter
	 * @param count - the amount by which to increment the counter
	 */
	public void increment(String ns, long count) {
		String namespace = (ns == null) ? noNSLabel : ns;
		counterMap.compute(namespace, (k, v) -> {
			if (v == null) {
				return new AtomicLong(count);
			} else {
				v.getAndAdd(count);
				return v;
			}
		});
	}

	/**
	 * Get the counter's total, which is the sum of the counter across all namespaces
	 *
	 * @return the total
	 */
	public long getTotal() {
		AtomicLong total = new AtomicLong();
		counterMap.forEach((k,v)-> {
			total.getAndAdd(v.longValue());
		});
		return total.get();
	}

	/**
	 * Get the counter's count for the provided namespace
	 *
	 * @param namespace the namespace for which we want the count
	 * @return the count for the namespace
	 */
	public long getCountByNS(String namespace) {
		AtomicLong count = counterMap.get(namespace);
		if (count == null) {
			return 0;
		}
		return counterMap.get(namespace).longValue();
	}
}
