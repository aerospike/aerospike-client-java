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

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.Command.KeyIter;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.proxy.client.KVSGrpc;
import com.aerospike.proxy.client.Kvs;

/**
 * Batch proxy commands.
 */
public class BatchProxy {
	public static final class OperateListCommand extends CommandProxy {
		private final BatchOperateListListener listener;
		private final BatchPolicy batchPolicy;
		private final List<BatchRecord> records;
		private boolean status;

		public OperateListCommand(
			GrpcCallExecutor grpcCallExecutor,
			BatchPolicy batchPolicy,
			BatchOperateListListener listener,
			List<BatchRecord> records
		) {
			super(KVSGrpc.getBatchOperateStreamingMethod(), grpcCallExecutor, batchPolicy, false);
			this.listener = listener;
			this.batchPolicy = batchPolicy;
			this.records = records;
			this.status = true;
		}

		@Override
		void writeCommand(Command command) {
			BatchRecordIterProxy iter = new BatchRecordIterProxy(records);
			command.setBatchOperate(batchPolicy, iter);
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
				try {
					listener.onSuccess(records, status);
				}
				catch (Throwable t) {
					logOnSuccessError(t);
				}
			}
			else {
				notifyFailure(new AerospikeException(resultCode));
			}
		}

		private void parse(Parser parser, Kvs.AerospikeResponsePayload response, int resultCode) {
			parser.skipKey();

			BatchRecord record = records.get(parser.batchIndex);

			if (resultCode == ResultCode.OK) {
				record.setRecord(parser.parseRecord(true));
				return;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parser.parseRecord(true);
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = inDoubt || response.getInDoubt();
					status = false;
					return;
				}
			}

			record.setError(resultCode, inDoubt || response.getInDoubt());
			status = false;
		}

		@Override
		void parseResult(Parser parser) {
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class OperateSequenceCommand extends CommandProxy {
		private final BatchRecordSequenceListener listener;
		private final BatchPolicy batchPolicy;
		private final List<BatchRecord> records;

		public OperateSequenceCommand(
			GrpcCallExecutor grpcCallExecutor,
			BatchPolicy batchPolicy,
			BatchRecordSequenceListener listener,
			List<BatchRecord> records
		) {
			super(KVSGrpc.getBatchOperateStreamingMethod(), grpcCallExecutor, batchPolicy, false);
			this.listener = listener;
			this.batchPolicy = batchPolicy;
			this.records = records;
		}

		@Override
		void writeCommand(Command command) {
			BatchRecordIterProxy iter = new BatchRecordIterProxy(records);
			command.setBatchOperate(batchPolicy, iter);
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
				try {
					listener.onSuccess();
				}
				catch (Throwable t) {
					logOnSuccessError(t);
				}
			}
			else {
				notifyFailure(new AerospikeException(resultCode));
			}
		}

		private void parse(Parser parser, Kvs.AerospikeResponsePayload response, int resultCode) {
			parser.skipKey();

			BatchRecord record = records.get(parser.batchIndex);

			if (resultCode == ResultCode.OK) {
				record.setRecord(parser.parseRecord(true));
			}
			else if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parser.parseRecord(true);
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = inDoubt || response.getInDoubt();
				}
				else {
					record.setError(resultCode, inDoubt || response.getInDoubt());
				}
			}
			else {
				record.setError(resultCode, inDoubt || response.getInDoubt());
			}

			try {
				listener.onRecord(record, parser.batchIndex);
			}
			catch (Throwable t) {
				logOnSuccessError(t);
			}
		}

		@Override
		void parseResult(Parser parser) {
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class OperateRecordArrayCommand extends CommandProxy {
		private final BatchRecordArrayListener listener;
		private final BatchRecord[] records;
		private final BatchPolicy batchPolicy;
		private final Key[] keys;
		private final Operation[] ops;
		private final BatchAttr attr;
		private final boolean isOperation;
		private boolean status;

		public OperateRecordArrayCommand(
			GrpcCallExecutor grpcCallExecutor,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecordArrayListener listener,
			BatchAttr attr
		) {
			super(KVSGrpc.getBatchOperateStreamingMethod(), grpcCallExecutor, batchPolicy, false);
			this.batchPolicy = batchPolicy;
			this.keys = keys;
			this.ops = ops;
			this.listener = listener;
			this.attr = attr;
			this.isOperation = ops != null;
			this.status = true;
			this.records = new BatchRecord[keys.length];

			for (int i = 0; i < keys.length; i++) {
				this.records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}
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
				try {
					listener.onSuccess(records, status);
				}
				catch (Throwable t) {
					logOnSuccessError(t);
				}
			}
			else {
				notifyFailure(new AerospikeException(resultCode));
			}
		}

		private void parse(Parser parser, Kvs.AerospikeResponsePayload response, int resultCode) {
			parser.skipKey();

			BatchRecord record = records[parser.batchIndex];

			if (resultCode == 0) {
				record.setRecord(parser.parseRecord(isOperation));
			}
			else {
				record.setError(resultCode, inDoubt || response.getInDoubt());
				status = false;
			}
		}

		@Override
		void parseResult(Parser parser) {
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(records, ae);
		}
	}

	public static final class OperateRecordSequenceCommand extends CommandProxy {
		private final BatchRecordSequenceListener listener;
		private final BatchPolicy batchPolicy;
		private final Key[] keys;
		private final Operation[] ops;
		private final BatchAttr attr;
		private final boolean isOperation;

		public OperateRecordSequenceCommand(
			GrpcCallExecutor grpcCallExecutor,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecordSequenceListener listener,
			BatchAttr attr
		) {
			super(KVSGrpc.getBatchOperateStreamingMethod(), grpcCallExecutor, batchPolicy, false);
			this.batchPolicy = batchPolicy;
			this.keys = keys;
			this.ops = ops;
			this.listener = listener;
			this.attr = attr;
			this.isOperation = ops != null;
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
				try {
					listener.onSuccess();
				}
				catch (Throwable t) {
					logOnSuccessError(t);
				}
			}
			else {
				notifyFailure(new AerospikeException(resultCode));
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

			try {
				listener.onRecord(record, parser.batchIndex);
			}
			catch (Throwable t) {
				logOnSuccessError(t);
			}
		}

		@Override
		void parseResult(Parser parser) {
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	private static class BatchRecordIterProxy implements KeyIter<BatchRecord> {
		private final List<? extends BatchRecord> records;
		private final int size;
		private int offset;
		private int index;

		public BatchRecordIterProxy(List<? extends BatchRecord> records) {
			this.records = records;
			this.size = records.size();
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public BatchRecord next() {
			if (index >= size) {
				return null;
			}
			offset = index++;
			return records.get(offset);
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

	private static class KeyIterProxy implements KeyIter<Key> {
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
