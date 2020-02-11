/*
 * Copyright 2012-2020 Aerospike, Inc.
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

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
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
	final boolean stopOnNotFound;

	/**
	 * Batch constructor.
	 */
	public AsyncMultiCommand(AsyncMultiExecutor parent, Node node, Policy policy) {
		super(policy, false);
		this.parent = parent;
		this.node = node;
		this.stopOnNotFound = false;
	}

	/**
	 * Scan/Query constructor.
	 */
	public AsyncMultiCommand(AsyncMultiExecutor parent, Node node, Policy policy, int socketTimeout, int totalTimeout) {
		super(policy, socketTimeout, totalTimeout);
		this.parent = parent;
		this.node = node;
		this.stopOnNotFound = true;
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
					if (stopOnNotFound) {
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

			Key key = parseKey();
			parseRow(key);
		}
		return false;
	}

	private final Key parseKey() {
		byte[] digest = null;
		String namespace = null;
		String setName = null;
		Value userKey = null;

		for (int i = 0; i < fieldCount; i++) {
			int fieldlen = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;

			int fieldtype = dataBuffer[dataOffset++];
			int size = fieldlen - 1;

			switch (fieldtype) {
			case FieldType.DIGEST_RIPE:
				digest = new byte[size];
				System.arraycopy(dataBuffer, dataOffset, digest, 0, size);
				dataOffset += size;
				break;

			case FieldType.NAMESPACE:
				namespace = Buffer.utf8ToString(dataBuffer, dataOffset, size);
				dataOffset += size;
				break;

			case FieldType.TABLE:
				setName = Buffer.utf8ToString(dataBuffer, dataOffset, size);
				dataOffset += size;
				break;

			case FieldType.KEY:
				int type = dataBuffer[dataOffset++];
				size--;
				userKey = Buffer.bytesToKeyValue(type, dataBuffer, dataOffset, size);
				dataOffset += size;
				break;
			}
		}
		return new Key(namespace, digest, setName, userKey);
	}

	protected final Record parseRecord() {
		Map<String,Object> bins = null;

		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(dataBuffer, dataOffset);
			byte particleType = dataBuffer[dataOffset+5];
			byte nameSize = dataBuffer[dataOffset+7];
			String name = Buffer.utf8ToString(dataBuffer, dataOffset+8, nameSize);
			dataOffset += 4 + 4 + nameSize;

			int particleBytesSize = (int) (opSize - (4 + nameSize));
	        Object value = Buffer.bytesToParticle(particleType, dataBuffer, dataOffset, particleBytesSize);
			dataOffset += particleBytesSize;

			if (bins == null) {
				bins = new HashMap<String,Object>();
			}
			bins.put(name, value);
	    }
	    return new Record(bins, generation, expiration);
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
