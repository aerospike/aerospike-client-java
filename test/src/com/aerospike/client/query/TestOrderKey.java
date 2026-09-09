/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.aerospike.client.query;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class TestOrderKey {
	private static final String BIN = "bin";

	@Test
	public void caseInsensitiveStringUsesAsciiFolding() {
		OrderKey ascii = key("K", BinDataType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE, "ascii");
		OrderKey kelvin = key("\u212a", BinDataType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE, "kelvin");

		assertTrue(ascii.compareTo(kelvin) < 0);
	}

	@Test
	public void nanUsesNaturalOrderBeforeDirection() {
		OrderKey finiteAscending = key(1.0, BinDataType.DOUBLE, Order.ASC, OrderByFlags.NONE, "finite");
		OrderKey nanAscending = key(Double.NaN, BinDataType.DOUBLE, Order.ASC, OrderByFlags.NONE, "nan");
		assertTrue(finiteAscending.compareTo(nanAscending) < 0);

		OrderKey finiteDescending = key(1.0, BinDataType.DOUBLE, Order.DESC, OrderByFlags.NONE, "finite");
		OrderKey nanDescending = key(Double.NaN, BinDataType.DOUBLE, Order.DESC, OrderByFlags.NONE, "nan");
		assertTrue(nanDescending.compareTo(finiteDescending) < 0);
	}

	private static OrderKey key(
		Object value,
		BinDataType type,
		Order order,
		OrderByFlags flags,
		String userKey
	) {
		Map<String, Object> bins = new HashMap<>();
		bins.put(BIN, value);
		Key key = new Key("test", "set", userKey);
		return new OrderKey(
			new Record(bins, 1, 0), BIN, type, order, flags, key.digest);
	}
}
