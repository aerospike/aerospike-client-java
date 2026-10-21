/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.client.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.aerospike.client.discovery.Endpoint.SourceList;

/**
 * U-8 timing half: {@code translate()} p99 stays under a 10 ms CI ceiling. The NFR-7 target is
 * 1 ms and is not asserted.
 */
public class TranslateLatencyTest {
	private static final int WARMUP = 200_000;
	private static final int ITERATIONS = 100_000;
	private static final long CEILING_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

	private static final Endpoint[] ENDPOINTS = new Endpoint[] {
		new Endpoint("BB9A", null, "10.0.0.1", 3000, SourceList.STANDARD),
		new Endpoint("BB9B", "tls1", "10.0.0.2", 3000, SourceList.STANDARD),
		new Endpoint("BB9C", null, "10.0.0.3", 3000, SourceList.ALTERNATE),
		new Endpoint("BB9D", null, "2001:db8::1", 3000, SourceList.STANDARD)
	};

	@Test
	public void staticMapP99UnderCeiling() {
		measure("static-map", new StaticMapAddressTranslator(Map.of("10.0.0.1", "192.168.1.1", "10.0.0.2", "192.168.1.2")), 2);
	}

	@Test
	public void portMapP99UnderCeiling() {
		measure("port-map", new PortMapAddressTranslator(Map.of("10.0.0.1:3000", "lb.example.com:30001",
			"[2001:db8::1]:3000", "lb.example.com:30002", "BB9B", "lb.example.com:30003")), 3);
	}

	@Test
	public void passThroughP99UnderCeiling() {
		measure("pass-through", PassThroughAddressTranslator.INSTANCE, 4);
	}

	private static void measure(String name, AddressTranslator translator, int hitsPerRound) {
		int hits = 0;

		for (int i = 0; i < WARMUP; i++) {
			if (translator.translate(ENDPOINTS[i % ENDPOINTS.length]).isPresent()) {
				hits++;
			}
		}

		long[] nanos = new long[ITERATIONS];

		for (int i = 0; i < ITERATIONS; i++) {
			Endpoint advertised = ENDPOINTS[i % ENDPOINTS.length];
			long begin = System.nanoTime();
			boolean hit = translator.translate(advertised).isPresent();
			nanos[i] = System.nanoTime() - begin;

			if (hit) {
				hits++;
			}
		}

		Arrays.sort(nanos);
		long p50 = nanos[ITERATIONS / 2];
		long p99 = nanos[ITERATIONS * 99 / 100];
		System.out.println("translate() " + name + ": p50 " + p50 + " ns, p99 " + p99 + " ns over " + ITERATIONS + " calls");

		assertEquals((WARMUP + ITERATIONS) / ENDPOINTS.length * hitsPerRound, hits);
		assertTrue(name + " p99 " + p99 + " ns", p99 < CEILING_NANOS);
	}
}
