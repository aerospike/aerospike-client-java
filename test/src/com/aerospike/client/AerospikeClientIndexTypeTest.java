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
package com.aerospike.client;

import static org.junit.Assert.assertSame;

import org.junit.Test;

import com.aerospike.client.query.IndexType;
import com.aerospike.client.util.Version;

/**
 * Server-independent unit tests for {@link AerospikeClient#resolveIndexType}.
 *
 * Server versions 8.1.3+ use the "integer" index type instead of "numeric".
 * The client transparently maps between the two based on the target server
 * version. This is the only place the mapping is observable: on the server,
 * "numeric" and "integer" collapse to the same internal type, so the created
 * index carries no record of which spelling was used.
 */
public class AerospikeClientIndexTypeTest {
	private static Version version(int major, int minor, int patch, int build) {
		return new Version(major, minor, patch, build);
	}

	@Test
	public void numericUpgradesToIntegerOn813() {
		// Exact boundary.
		assertSame(IndexType.INTEGER, AerospikeClient.resolveIndexType(IndexType.NUMERIC, version(8, 1, 3, 0)));
	}

	@Test
	public void numericUpgradesToIntegerAbove813() {
		assertSame(IndexType.INTEGER, AerospikeClient.resolveIndexType(IndexType.NUMERIC, version(8, 1, 4, 0)));
		assertSame(IndexType.INTEGER, AerospikeClient.resolveIndexType(IndexType.NUMERIC, version(9, 0, 0, 0)));
		// Build component past the boundary still counts as >= 8.1.3.0.
		assertSame(IndexType.INTEGER, AerospikeClient.resolveIndexType(IndexType.NUMERIC, version(8, 1, 3, 5)));
	}

	@Test
	public void numericUnchangedBelow813() {
		assertSame(IndexType.NUMERIC, AerospikeClient.resolveIndexType(IndexType.NUMERIC, version(8, 1, 2, 0)));
		assertSame(IndexType.NUMERIC, AerospikeClient.resolveIndexType(IndexType.NUMERIC, version(8, 1, 2, 99)));
		assertSame(IndexType.NUMERIC, AerospikeClient.resolveIndexType(IndexType.NUMERIC, version(8, 0, 0, 0)));
		assertSame(IndexType.NUMERIC, AerospikeClient.resolveIndexType(IndexType.NUMERIC, version(4, 9, 0, 3)));
	}

	@Test
	public void integerUnchangedOnOrAbove813() {
		assertSame(IndexType.INTEGER, AerospikeClient.resolveIndexType(IndexType.INTEGER, version(8, 1, 3, 0)));
		assertSame(IndexType.INTEGER, AerospikeClient.resolveIndexType(IndexType.INTEGER, version(9, 0, 0, 0)));
	}

	@Test
	public void integerDowngradesToNumericBelow813() {
		assertSame(IndexType.NUMERIC, AerospikeClient.resolveIndexType(IndexType.INTEGER, version(8, 1, 2, 0)));
		assertSame(IndexType.NUMERIC, AerospikeClient.resolveIndexType(IndexType.INTEGER, version(8, 0, 0, 0)));
		assertSame(IndexType.NUMERIC, AerospikeClient.resolveIndexType(IndexType.INTEGER, version(4, 9, 0, 3)));
	}

	@Test
	public void otherTypesUnchangedAcrossVersions() {
		for (IndexType type : new IndexType[] {IndexType.STRING, IndexType.GEO2DSPHERE}) {
			assertSame(type, AerospikeClient.resolveIndexType(type, version(8, 1, 2, 0)));
			assertSame(type, AerospikeClient.resolveIndexType(type, version(8, 1, 3, 0)));
			assertSame(type, AerospikeClient.resolveIndexType(type, version(9, 0, 0, 0)));
		}
	}
}
