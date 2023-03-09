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
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.proxy.client.KVSGrpc;
import com.aerospike.proxy.client.Kvs;

/**
 * Batch proxy commands.
 */
public class BatchProxy {
	//-------------------------------------------------------
	// Exists
	//-------------------------------------------------------

	public static final class ExistsArrayCommand extends BaseCommand {
		private final ExistsArrayListener listener;
		private final Key[] keys;
		private final boolean[] existsArray;

		public ExistsArrayCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			ExistsArrayListener listener,
			Key[] keys
		) {
			super(executor, batchPolicy);
			this.listener = listener;
			this.keys = keys;
			this.existsArray = new boolean[keys.length];
		}

		@Override
		void writeCommand(Command command) {
			BatchAttr attr = new BatchAttr(batchPolicy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			KeyIterProxy iter = new KeyIterProxy(keys);
			command.setBatchOperate(batchPolicy, iter, null, null, attr);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			if (parser.opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}
			existsArray[parser.batchIndex] = resultCode == 0;
		}

		@Override
		void onSuccess() {
			listener.onSuccess(keys, existsArray);
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class ExistsSequenceCommand extends BaseCommand {
		private final ExistsSequenceListener listener;
		private final Key[] keys;

		public ExistsSequenceCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			ExistsSequenceListener listener,
			Key[] keys
		) {
			super(executor, batchPolicy);
			this.listener = listener;
			this.keys = keys;
		}

		@Override
		void writeCommand(Command command) {
			BatchAttr attr = new BatchAttr(batchPolicy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			KeyIterProxy iter = new KeyIterProxy(keys);
			command.setBatchOperate(batchPolicy, iter, null, null, attr);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			if (parser.opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}
			listener.onExists(keys[parser.batchIndex], resultCode == 0);
		}

		@Override
		void onSuccess() {
			listener.onSuccess();
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	//-------------------------------------------------------
	// Operate on List<BatchRecord>
	//-------------------------------------------------------

	public static final class OperateListCommand extends BaseCommand {
		private final BatchOperateListListener listener;
		private final List<BatchRecord> records;
		private boolean status;

		public OperateListCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			BatchOperateListListener listener,
			List<BatchRecord> records
		) {
			super(executor, batchPolicy);
			this.listener = listener;
			this.records = records;
			this.status = true;
		}

		@Override
		void writeCommand(Command command) {
			BatchRecordIterProxy iter = new BatchRecordIterProxy(records);
			command.setBatchOperate(batchPolicy, iter);
		}

		@Override
		void parse(Parser parser, int resultCode) {
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
					record.inDoubt = inDoubt;
					status = false;
					return;
				}
			}

			record.setError(resultCode, inDoubt);
			status = false;
		}

		@Override
		void onSuccess() {
			listener.onSuccess(records, status);
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class OperateSequenceCommand extends BaseCommand {
		private final BatchRecordSequenceListener listener;
		private final List<BatchRecord> records;

		public OperateSequenceCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			BatchRecordSequenceListener listener,
			List<BatchRecord> records
		) {
			super(executor, batchPolicy);
			this.listener = listener;
			this.records = records;
		}

		@Override
		void writeCommand(Command command) {
			BatchRecordIterProxy iter = new BatchRecordIterProxy(records);
			command.setBatchOperate(batchPolicy, iter);
		}

		@Override
		void parse(Parser parser, int resultCode) {
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
					record.inDoubt = inDoubt;
				}
				else {
					record.setError(resultCode, inDoubt);
				}
			}
			else {
				record.setError(resultCode, inDoubt);
			}

			listener.onRecord(record, parser.batchIndex);
		}

		@Override
		void onSuccess() {
			listener.onSuccess();
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	//-------------------------------------------------------
	// Operate on Key[] keys
	//-------------------------------------------------------

	public static final class OperateRecordArrayCommand extends BaseCommand {
		private final BatchRecordArrayListener listener;
		private final BatchRecord[] records;
		private final Key[] keys;
		private final Operation[] ops;
		private final BatchAttr attr;
		private final boolean isOperation;
		private boolean status;

		public OperateRecordArrayCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecordArrayListener listener,
			BatchAttr attr
		) {
			super(executor, batchPolicy);
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
		void parse(Parser parser, int resultCode) {
			BatchRecord record = records[parser.batchIndex];

			if (resultCode == 0) {
				record.setRecord(parser.parseRecord(isOperation));
			}
			else {
				record.setError(resultCode, inDoubt);
				status = false;
			}
		}

		@Override
		void onSuccess() {
			listener.onSuccess(records, status);
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(records, ae);
		}
	}

	public static final class OperateRecordSequenceCommand extends BaseCommand {
		private final BatchRecordSequenceListener listener;
		private final Key[] keys;
		private final Operation[] ops;
		private final BatchAttr attr;
		private final boolean isOperation;

		public OperateRecordSequenceCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecordSequenceListener listener,
			BatchAttr attr
		) {
			super(executor, batchPolicy);
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
		void parse(Parser parser, int resultCode) {
			Key keyOrig = keys[parser.batchIndex];
			BatchRecord record;

			if (resultCode == ResultCode.OK) {
				record = new BatchRecord(keyOrig, parser.parseRecord(isOperation), attr.hasWrite);
			}
			else {
				record = new BatchRecord(keyOrig, null, resultCode, inDoubt, attr.hasWrite);
			}

			listener.onRecord(record, parser.batchIndex);
		}

		@Override
		void onSuccess() {
			listener.onSuccess();
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	//-------------------------------------------------------
	// Base Batch Proxy Command
	//-------------------------------------------------------

	private static abstract class BaseCommand extends CommandProxy {
		final BatchPolicy batchPolicy;

		public BaseCommand(GrpcCallExecutor executor, BatchPolicy batchPolicy) {
			super(KVSGrpc.getBatchOperateStreamingMethod(), executor, batchPolicy, false);
			this.batchPolicy = batchPolicy;
		}

		@Override
		final void onResponse(Kvs.AerospikeResponsePayload response) {
			byte[] bytes = response.getPayload().toByteArray();
			Parser parser = new Parser(bytes, 5);

			int resultCode = parser.parseHeader();

			if (response.getHasNext()) {
				setInDoubt(response.getInDoubt());
				parser.skipKey();
				parse(parser, resultCode);
				return;
			}

			if (resultCode == ResultCode.OK) {
				try {
					onSuccess();
				}
				catch (Throwable t) {
					logOnSuccessError(t);
				}
			}
			else {
				notifyFailure(new AerospikeException(resultCode));
			}
		}

		@Override
		final void parseResult(Parser parser) {
		}

		abstract void parse(Parser parser, int resultCode);
		abstract void onSuccess();
	}

	//-------------------------------------------------------
	// Proxy Iterators
	//-------------------------------------------------------

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
