/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.client.proxy;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.Command.KeyIter;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.util.Util;
import com.aerospike.proxy.client.KVSGrpc;
import com.aerospike.proxy.client.Kvs;

/**
 * All batch executors in one class mimicking
 * {@link com.aerospike.client.async.AsyncBatch}.
 */
public class BatchProxy {
	//-------------------------------------------------------
	// OperateRecordSequence
	//-------------------------------------------------------

	public static final class OperateRecordSequenceCommandProxy extends CommandProxy {
		private final BatchRecordSequenceListener listener;
		private final boolean[] sent;
		private final BatchPolicy batchPolicy;
		private final Key[] keys;
		private final Operation[] ops;
		private final BatchAttr attr;
		private final boolean isOperation;

		public OperateRecordSequenceCommandProxy(
			GrpcCallExecutor grpcCallExecutor,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecordSequenceListener listener,
			BatchAttr attr
		) {
			super(KVSGrpc.getBatchOperateStreamingMethod(), grpcCallExecutor, batchPolicy);
			this.batchPolicy = batchPolicy;
			this.keys = keys;
			this.ops = ops;
			this.sent = new boolean[keys.length];
			this.listener = listener;
			this.attr = attr;
			this.isOperation = ops != null;
		}

		@Override
		boolean isUnaryCall() {
			return false;
		}

		@Override
		void writeCommand(Command command) {
			KeyIterProxy iter = new KeyIterProxy(keys);
			command.setBatchOperate(batchPolicy, iter, null, ops, attr);
		}

		@Override
		void onResponse(Kvs.AerospikeResponsePayload response) {
			byte[] bytes = response.getPayload().toByteArray();
			Parser parser = new Parser(bytes, 5);

			int resultCode = parser.parseHeader();

			if (response.getHasNext()) {
				parse(parser, response, resultCode);
				return;
			}

			if (resultCode == ResultCode.OK) {
				onSuccess();
			}
			else {
				onFailure(new AerospikeException(resultCode));
			}
		}

		private void parse(Parser parser, Kvs.AerospikeResponsePayload response, int resultCode) {
			parser.skipKey();

			Key keyOrig = keys[parser.batchIndex];
			BatchRecord record;

			if (resultCode == ResultCode.OK) {
				record = new BatchRecord(keyOrig, parser.parseRecord(isOperation), attr.hasWrite);
			}
			else {
				record = new BatchRecord(keyOrig, null, resultCode, inDoubt || response.getInDoubt(), attr.hasWrite);
			}
			sent[parser.batchIndex] = true;

			try {
				listener.onRecord(record, parser.batchIndex);
			}
			catch (Throwable e) {
				Log.error("Unexpected exception from onRecord(): " + Util.getErrorMessage(e));
			}
		}

		@Override
		void parseResult(Parser parser) {
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}

		private void onSuccess() {
			try {
				listener.onSuccess();
			}
			catch (Throwable t) {
				logOnSuccessError(t);
			}
		}
	}

	private static class KeyIterProxy implements KeyIter {
		private final Key[] keys;
		private int offset;
		private int index;

		public KeyIterProxy(Key[] keys) {
			this.keys = keys;
		}

		@Override
		public int size() {
			return keys.length;
		}

		@Override
		public Key next() {
			if (index >= keys.length) {
				return null;
			}
			offset = index++;
			return keys[offset];
		}

		@Override
		public int offset() {
			return offset;
		}

		@Override
		public void reset() {
			index = 0;
		}
	}
}
