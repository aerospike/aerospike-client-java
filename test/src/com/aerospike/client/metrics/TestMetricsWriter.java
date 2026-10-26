/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class TestMetricsWriter {
	@Test
	public void testWriterIsExporterOnly() {
		assertTrue(IMetricsExporter.class.isAssignableFrom(MetricsWriter.class));
		assertFalse(MetricsListener.class.isAssignableFrom(MetricsWriter.class));
	}

	@Test
	public void testLazyOpenFlushAndLegacyGrammar() throws Exception {
		Path directory = Files.createTempDirectory("metrics-writer-");

		try {
			MetricsWriter writer = new MetricsWriter(directory.toString());
			writer.configure(new MetricsPolicy());
			try (java.util.stream.Stream<Path> files = Files.list(directory)) {
				assertEquals(0, files.count());
			}

			writer.export(snapshot());
			Path output;
			try (java.util.stream.Stream<Path> files = Files.list(directory)) {
				output = files.findFirst().orElseThrow();
			}
			String contents = Files.readString(output);

			assertTrue(contents.contains("header(2)"));
			assertTrue(contents.contains(" cluster[cluster,java,1.0,app,"));
			writer.closeWriter();

			// A disabled writer reopens lazily when it is used after re-enable.
			writer.export(snapshot());
			writer.closeWriter();
		}
		finally {
			try (java.util.stream.Stream<Path> files = Files.list(directory)) {
				files.forEach(path -> {
					try {
						Files.delete(path);
					}
					catch (Exception ignored) {
					}
				});
			}
			Files.delete(directory);
		}
	}

	@Test
	public void testWriterUsesSnapshotTimestampForClusterRecord() throws Exception {
		Path directory = Files.createTempDirectory("metrics-writer-timestamp-");
		Instant timestamp = Instant.parse("2001-02-03T04:05:06.789Z");

		try {
			MetricsWriter writer = new MetricsWriter(directory.toString());
			writer.export(snapshot(timestamp));
			writer.closeWriter();

			Path output;
			try (java.util.stream.Stream<Path> files = Files.list(directory)) {
				output = files.findFirst().orElseThrow();
			}
			List<String> lines = Files.readAllLines(output);
			String expectedTimestamp = LocalDateTime.ofInstant(timestamp, ZoneId.systemDefault())
				.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

			assertEquals(2, lines.size());
			assertTrue(lines.get(1).startsWith(expectedTimestamp + " cluster["));
		}
		finally {
			try (java.util.stream.Stream<Path> files = Files.list(directory)) {
				files.forEach(path -> {
					try {
						Files.delete(path);
					}
					catch (Exception ignored) {
					}
				});
			}
			Files.delete(directory);
		}
	}

	private static MetricsSnapshot snapshot() {
		return snapshot(Instant.EPOCH);
	}

	private static MetricsSnapshot snapshot(Instant timestamp) {
		return new MetricsSnapshot(
			timestamp,
			true,
			true,
			false,
			"cluster",
			"java",
			"1.0",
			"app",
			Collections.emptyMap(),
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0.0, 0,
			null, null, null, null,
			0, 0, 0,
			Collections.emptyList(),
			Collections.emptyList(),
			Collections.emptyList(),
			new UsageMetricsSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
			LatencyUnit.MILLISECONDS,
			7,
			1
		);
	}
}
