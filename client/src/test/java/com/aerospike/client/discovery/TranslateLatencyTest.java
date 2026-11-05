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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.aerospike.client.discovery.Endpoint.SourceList;

/**
 * {@code translate()} p99 stays under 1 ms.
 * <p>
 * Each translator is timed over {@link #ROUNDS} rounds after a warm-up, and the test asserts that
 * the median of the round p99s is under 1 ms. A GC pause or scheduler stall disturbs only the
 * rounds it overlaps, and the median ignores up to three disturbed rounds. A translator whose p99
 * is above 1 ms puts every round p99 above 1 ms, so the median is above it too.
 */
public class TranslateLatencyTest {
	private static final int WARMUP = 200_000;
	private static final int ROUNDS = 7;
	private static final int CALLS_PER_ROUND = 20_000;
	private static final long TARGET_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

	private static final Endpoint[] ENDPOINTS = new Endpoint[] {
		new Endpoint("BB9A", null, "10.0.0.1", 3000, SourceList.STANDARD),
		new Endpoint("BB9B", "tls1", "10.0.0.2", 3000, SourceList.STANDARD),
		new Endpoint("BB9C", null, "10.0.0.3", 3000, SourceList.ALTERNATE),
		new Endpoint("BB9D", null, "2001:db8::1", 3000, SourceList.STANDARD)
	};

	@Test
	public void staticMapP99UnderTarget() {
		measure("static-map", new StaticMapAddressTranslator(Map.of("10.0.0.1", "192.168.1.1", "10.0.0.2", "192.168.1.2")), 2);
	}

	@Test
	public void portMapP99UnderTarget() {
		measure("port-map", new PortMapAddressTranslator(Map.of("10.0.0.1:3000", "lb.example.com:30001",
			"[2001:db8::1]:3000", "lb.example.com:30002", "BB9B", "lb.example.com:30003")), 3);
	}

	@Test
	public void passThroughP99UnderTarget() {
		measure("pass-through", PassThroughAddressTranslator.INSTANCE, 4);
	}

	@Test
	public void spinningTranslatorFailsTarget() {
		long spinNanos = TimeUnit.MILLISECONDS.toNanos(2);
		AddressTranslator spinning = advertised -> {
			long end = System.nanoTime() + spinNanos;

			while (System.nanoTime() < end) {
				Thread.onSpinWait();
			}
			return Optional.empty();
		};

		long[][] rounds = new long[ROUNDS][100];

		for (long[] round : rounds) {
			time(spinning, round);
		}

		AssertionError failure = assertThrows(AssertionError.class, () -> assertP99UnderTarget("spin-2ms", rounds));
		assertTrue(failure.getMessage(), failure.getMessage().startsWith("spin-2ms median round p99 "));
	}

	private static void measure(String name, AddressTranslator translator, int hitsPerRound) {
		int hits = time(translator, new long[WARMUP]);
		long[][] rounds = new long[ROUNDS][CALLS_PER_ROUND];

		for (long[] round : rounds) {
			hits += time(translator, round);
		}

		assertEquals((WARMUP + ROUNDS * CALLS_PER_ROUND) / ENDPOINTS.length * hitsPerRound, hits);
		assertP99UnderTarget(name, rounds);
	}

	private static int time(AddressTranslator translator, long[] nanos) {
		int hits = 0;

		for (int i = 0; i < nanos.length; i++) {
			Endpoint advertised = ENDPOINTS[i % ENDPOINTS.length];
			long begin = System.nanoTime();
			boolean hit = translator.translate(advertised).isPresent();
			nanos[i] = System.nanoTime() - begin;

			if (hit) {
				hits++;
			}
		}
		return hits;
	}

	private static void assertP99UnderTarget(String name, long[][] rounds) {
		long[] p50s = new long[rounds.length];
		long[] p99s = new long[rounds.length];
		int calls = 0;

		for (int r = 0; r < rounds.length; r++) {
			long[] nanos = rounds[r];
			Arrays.sort(nanos);
			p50s[r] = nanos[nanos.length / 2];
			p99s[r] = nanos[nanos.length * 99 / 100];
			calls += nanos.length;
		}

		String roundP99s = Arrays.toString(p99s);
		Arrays.sort(p50s);
		Arrays.sort(p99s);
		long p50 = p50s[rounds.length / 2];
		long p99 = p99s[rounds.length / 2];
		System.out.println("translate() " + name + ": p50 " + p50 + " ns, p99 " + p99 + " ns (median of " + rounds.length +
			" rounds, round p99s " + roundP99s + " ns) over " + calls + " calls");

		assertTrue(name + " median round p99 " + p99 + " ns, round p99s " + roundP99s + " ns", p99 < TARGET_NANOS);
	}
}
