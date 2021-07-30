/*
 * Copyright 2012-2021 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;

public abstract class AsyncMultiCommand extends AsyncCommand {

	final AsyncMultiExecutor parent;
	final Node node;
	int groups;
	int info3;
	int resultCode;
	int generation;
	int expiration;
	int batchIndex;
	int fieldCount;
	int opCount;
	private final boolean isBatch;
	protected final boolean isOperation;

	/**
	 * Batch constructor.
	 */
	public AsyncMultiCommand(AsyncMultiExecutor parent, Node node, Policy policy, boolean isOperation) {
		super(policy, false);
		this.parent = parent;
		this.node = node;
		this.isBatch = true;
		this.isOperation = isOperation;
	}

	/**
	 * Scan/Query constructor.
	 */
	public AsyncMultiCommand(AsyncMultiExecutor parent, Node node, Policy policy, int socketTimeout, int totalTimeout) {
		super(policy, socketTimeout, totalTimeout);
		this.parent = parent;
		this.node = node;
		this.isBatch = false;
		this.isOperation = false;
	}

	@Override
	protected Node getNode(Cluster cluster) {
		return node;
	}

	@Override
	protected boolean prepareRetry(boolean timeout) {
		return true;
	}

	@Override
	final boolean parseResult() {
		while (dataOffset < receiveSize) {
			dataOffset += 3;
			info3 = dataBuffer[dataOffset] & 0xFF;
			dataOffset += 2;
			resultCode = dataBuffer[dataOffset] & 0xFF;

			if (resultCode != 0) {
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR || resultCode == ResultCode.FILTERED_OUT) {
					if (! isBatch) {
						return true;
					}
				}
				else {
					throw new AerospikeException(resultCode);
				}
			}

			// If this is the end marker of the response, do not proceed further
			if ((info3 & Command.INFO3_LAST) != 0) {
				return true;
			}
			dataOffset++;
			generation = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			expiration = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			batchIndex = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			fieldCount = Buffer.bytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;
			opCount = Buffer.bytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;

			if (isBatch) {
				skipKey(fieldCount);
				parseRow(null);
			}
			else {
				Key key = parseKey(fieldCount);
				parseRow(key);
			}
		}
		return false;
	}

	protected final Record parseRecord() {
		if (opCount <= 0) {
			return new Record(null, generation, expiration);
		}

		return parseRecord(opCount, generation, expiration, isOperation);
	}

	@Override
	protected final void onSuccess() {
		parent.childSuccess(node);
	}

	@Override
	protected void onFailure(AerospikeException e) {
		parent.childFailure(e);
	}

	protected abstract void parseRow(Key key);
}
