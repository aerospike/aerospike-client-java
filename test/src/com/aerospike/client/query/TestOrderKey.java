/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.aerospike.client.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class TestOrderKey {
	private static final String BIN = "bin";

	@Test
	public void nanUsesNaturalOrderBeforeDirection() {
		OrderKey finiteAscending = key(1.0, BinDataType.DOUBLE, Order.ASC, "finite");
		OrderKey nanAscending = key(Double.NaN, BinDataType.DOUBLE, Order.ASC, "nan");
		assertTrue(finiteAscending.compareTo(nanAscending) < 0);

		OrderKey finiteDescending = key(1.0, BinDataType.DOUBLE, Order.DESC, "finite");
		OrderKey nanDescending = key(Double.NaN, BinDataType.DOUBLE, Order.DESC, "nan");
		assertTrue(nanDescending.compareTo(finiteDescending) < 0);
	}

	@Test
	public void negativeZeroEqualsZeroAndDigestDecides() {
		for (Order order : Order.values()) {
			OrderKey negative = key(-0.0, BinDataType.DOUBLE, order, "a");
			OrderKey positive = key(0.0, BinDataType.DOUBLE, order, "b");
			OrderKey sameValue = key(0.0, BinDataType.DOUBLE, order, "a");

			assertEquals(Integer.signum(sameValue.compareTo(positive)),
				Integer.signum(negative.compareTo(positive)));
		}
	}

	private static OrderKey key(
		Object value,
		BinDataType type,
		Order order,
		String userKey
	) {
		Map<String, Object> bins = new HashMap<>();
		bins.put(BIN, value);
		Key key = new Key("test", "set", userKey);
		return new OrderKey(
			new Record(bins, 1, 0), new OrderBy[] {new OrderBy(BIN, type, order)}, key, 0);
	}
}
