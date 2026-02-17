/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.async;

import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;

public final class AsyncInfoCommand extends AsyncCommand {
	private final InfoListener listener;
	private final Node node;
	private final String[] commands;
	private Map<String,String> map;

	public AsyncInfoCommand(InfoListener listener, InfoPolicy policy, Node node, String... commands) {
		super(createPolicy(policy), true, null);
		this.listener = listener;
		this.node = node;
		this.commands = commands;
	}

	private static Policy createPolicy(InfoPolicy policy) {
		Policy p = new Policy();

		if (policy == null) {
			p.setTimeout(1000);
		}
		else {
			p.setTimeout(policy.timeout);
		}
		return p;
	}

	@Override
	boolean isWrite() {
		return true;
	}

	@Override
	Node getNode(Cluster cluster) {
		return node;
	}

	@Override
	protected LatencyType getLatencyType() {
		return LatencyType.NONE;
	}

	@Override
	protected void writeBuffer() {
		dataOffset = 8;

		for (String command : commands) {
			dataOffset += Buffer.estimateSizeUtf8(command) + 1;
		}

		sizeBuffer();
		dataOffset = 8; // Skip size field.

		// The command format is: <name1>\n<name2>\n...
		for (String command : commands) {
			dataOffset += Buffer.stringToUtf8(command, dataBuffer, dataOffset);
			dataBuffer[dataOffset++] = '\n';
		}

		// Write size field.
		long size = ((long)dataOffset - 8L) | (2L << 56) | (1L << 48);
		Buffer.longToBytes(size, dataBuffer, 0);
	}

	@Override
	protected final boolean parseResult() {
		Info info = new Info(dataBuffer, receiveSize);
		map = info.parseMultiResponse();
		return true;
	}

	@Override
	protected boolean prepareRetry(boolean timeout) {
		return true;
	}

	@Override
	protected void onSuccess() {
		if (listener != null) {
			listener.onSuccess(map);
		}
	}

	@Override
	protected void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
