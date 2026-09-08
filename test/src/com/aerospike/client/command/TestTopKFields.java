/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.aerospike.client.command;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.aerospike.client.query.BinDataType;
import com.aerospike.client.query.Order;
import com.aerospike.client.query.OrderByFlags;
import com.aerospike.client.query.Statement;

public class TestTopKFields {
	@Test
	public void orderByField() {
		Statement statement = new Statement();
		statement.setOrderBy("éx", BinDataType.STRING, Order.DESC, OrderByFlags.CASE_INSENSITIVE);
		statement.setTopK(1000);

		assertArrayEquals(
			new byte[] {3, 1, 1, 3, (byte)0xc3, (byte)0xa9, 'x'},
			Command.getTopKOrderByField(statement));
	}

	@Test
	public void topKField() {
		assertEquals(46, FieldType.ORDER_BY);
		assertEquals(47, FieldType.TOP_K);
		assertArrayEquals(new byte[] {0, 0, 3, (byte)0xe8}, Command.getTopKField(1000));
	}

	@Test
	public void fallbackDoesNotCreateFields() {
		Statement statement = new Statement();
		statement.setOrderBy("rank", BinDataType.INTEGER, Order.ASC);
		statement.setTopK(1);

		assertNull(Command.getTopKFields(statement, false));
	}
}
