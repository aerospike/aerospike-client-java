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
package com.aerospike.test.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

/**
 * The L5 driver's failure and ready lines. Needs no server.
 */
public class TestL5DriverFailure {
	private static final Pattern FAILURE = Pattern.compile(
		"\\{\"type\":\"failure\",\"t\":(\\d+),\"startedAt\":(\\d+),\"message\":\"(.*)\"\\}");
	private static final Pattern T = Pattern.compile("\"t\":(\\d+)");

	@Test
	public void failureHasTypeTimesAndMessage() {
		long startedAt = L5Driver.now();
		long t = L5Driver.now();
		Matcher m = FAILURE.matcher(L5Driver.failure(t, startedAt, "Error -11,1,0,0,0,0: Max retries exceeded: 0"));

		assertTrue(m.matches());
		assertEquals(t, Long.parseLong(m.group(1)));
		assertEquals(startedAt, Long.parseLong(m.group(2)));
		assertTrue(Long.parseLong(m.group(2)) <= Long.parseLong(m.group(1)));
		assertEquals("Error -11,1,0,0,0,0: Max retries exceeded: 0", m.group(3));
	}

	@Test
	public void failureOnlyAfterReady() {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		L5Driver.Output out = new L5Driver.Output(stream(bytes), stream(new ByteArrayOutputStream()));
		String ready = L5Driver.ready(5, 1700000000000L, "10.4.0", "port-map");

		out.writeIfOpen(t -> L5Driver.failure(3, 1, "before"));
		assertEquals("", bytes.toString(StandardCharsets.UTF_8));

		out.open(t -> ready);
		out.writeIfOpen(t -> L5Driver.failure(9, 6, "after"));
		assertEquals(ready + "\n" + L5Driver.failure(9, 6, "after") + "\n", bytes.toString(StandardCharsets.UTF_8));
	}

	@Test
	public void linesBeforeReadyGoToStderr() {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		ByteArrayOutputStream err = new ByteArrayOutputStream();
		L5Driver.Output out = new L5Driver.Output(stream(bytes), stream(err));
		String log = "{\"type\":\"log\",\"t\":3,\"level\":\"WARN\",\"message\":\"before\"}";
		String ready = L5Driver.ready(5, 1700000000000L, "10.4.0", "port-map");

		out.write(t -> log);
		out.open(t -> ready);

		assertEquals(ready + "\n", bytes.toString(StandardCharsets.UTF_8));
		assertTrue(err.toString(StandardCharsets.UTF_8).contains(log));
	}

	@Test
	public void tNeverGoesBackAcrossWriters() throws InterruptedException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		L5Driver.Output out = new L5Driver.Output(stream(bytes), stream(new ByteArrayOutputStream()));
		out.open(t -> L5Driver.ready(t, 1700000000000L, "10.4.0", "port-map"));
		Thread[] writers = new Thread[8];

		for (int i = 0; i < writers.length; i++) {
			writers[i] = new Thread(() -> {
				for (int j = 0; j < 2000; j++) {
					out.writeIfOpen(t -> L5Driver.failure(t, t, "x"));
				}
			});
			writers[i].start();
		}

		for (Thread writer : writers) {
			writer.join();
		}
		Matcher m = T.matcher(bytes.toString(StandardCharsets.UTF_8));
		long last = -1;
		int lines = 0;

		while (m.find()) {
			long t = Long.parseLong(m.group(1));
			assertTrue("t went back from " + last + " to " + t, t >= last);
			last = t;
			lines++;
		}
		assertEquals(1 + writers.length * 2000, lines);
	}

	@Test
	public void readyReportsContract3() {
		assertEquals("{\"type\":\"ready\",\"t\":5,\"contract\":3,\"epochMs\":1700000000000,\"client\":\"java\"," +
			"\"clientVersion\":\"10.4.0\",\"shape\":\"port-map\"}", L5Driver.ready(5, 1700000000000L, "10.4.0", "port-map"));
	}

	private static PrintStream stream(ByteArrayOutputStream bytes) {
		return new PrintStream(bytes, true, StandardCharsets.UTF_8);
	}
}
