/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.aerospike.test.sync.query;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.BinDataType;
import com.aerospike.client.query.Order;
import com.aerospike.client.query.Statement;
import com.aerospike.test.sync.TestSync;

public class TestTopKRequestEncoding extends TestSync {
	@Test
	public void topKFieldsAreEncodedOnlyForPushdown() {
		Cluster cluster = ((AerospikeClient)client).getCluster();
		Node node = cluster.getRandomNode();
		Statement statement = statement();

		Map<Integer, byte[]> pushed = fields(build(cluster, node, statement, true));
		assertEquals(6, pushed.size());
		assertArrayEquals(new byte[] {1, 1, 0, 4, 'r', 'a', 'n', 'k'}, pushed.get(FieldType.ORDER_BY));
		assertArrayEquals(new byte[] {0, 0, 0, 3}, pushed.get(FieldType.TOP_K));

		Map<Integer, byte[]> fallback = fields(build(cluster, node, statement, false));
		assertEquals(4, fallback.size());
		assertFalse(fallback.containsKey(FieldType.ORDER_BY));
		assertFalse(fallback.containsKey(FieldType.TOP_K));
	}

	private Statement statement() {
		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName("topKWire");
		statement.setBinNames("rank");
		statement.setOrderBy("rank", BinDataType.INTEGER, Order.DESC);
		statement.setTopK(3);
		return statement;
	}

	private byte[] build(Cluster cluster, Node node, Statement statement, boolean sendTopK) {
		Command command = new TestCommand();
		command.setQuery(cluster, new QueryPolicy(), statement, 1, false, null, node, sendTopK);
		return Arrays.copyOf(command.dataBuffer, command.dataOffset);
	}

	private Map<Integer, byte[]> fields(byte[] buffer) {
		int fieldCount = ((buffer[26] & 0xff) << 8) | (buffer[27] & 0xff);
		Map<Integer, byte[]> fields = new HashMap<>();
		int offset = Command.MSG_TOTAL_HEADER_SIZE;

		for (int i = 0; i < fieldCount; i++) {
			int size = Buffer.bytesToInt(buffer, offset);
			int type = buffer[offset + 4] & 0xff;
			fields.put(type, Arrays.copyOfRange(buffer, offset + 5, offset + 4 + size));
			offset += 4 + size;
		}

		assertEquals(buffer.length, offset + Command.OPERATION_HEADER_SIZE + 4);
		assertTrue(fields.containsKey(FieldType.NAMESPACE));
		assertTrue(fields.containsKey(FieldType.TABLE));
		return fields;
	}

	private static final class TestCommand extends Command {
		private TestCommand() {
			super(0, 0, 0);
		}

		@Override
		protected void sizeBuffer() {
			dataBuffer = new byte[dataOffset];
		}
	}
}
