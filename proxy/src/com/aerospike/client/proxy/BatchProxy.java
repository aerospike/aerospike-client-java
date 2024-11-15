/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.proxy.client.KVSGrpc;
import com.aerospike.proxy.client.Kvs;

/**
 * Batch proxy commands.
 */
public class BatchProxy {
	//-------------------------------------------------------
	// Batch Read Record List
	//-------------------------------------------------------

	public interface BatchListListenerSync {
		public void onSuccess(List<BatchRead> records, boolean status);
		public void onFailure(AerospikeException ae);
	}

	public static final class ReadListCommandSync extends BaseCommand {
		private final BatchListListenerSync listener;
		private final List<BatchRead> records;
		private boolean status;

		public ReadListCommandSync(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			BatchListListenerSync listener,
			List<BatchRead> records
		) {
			super(executor, batchPolicy, true, records.size());
			this.listener = listener;
			this.records = records;
			this.status = true;
		}

		@Override
		void writeCommand(Command command) {
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(records.size());
			command.setBatchOperate(batchPolicy, null, null, null, records, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			BatchRead record = records.get(parser.batchIndex);

			if (resultCode == 0) {
				record.setRecord(parseRecord(parser));
			}
			else {
				record.setError(resultCode, false);
				status = false;
			}
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

	public static final class ReadListCommand extends BaseCommand {
		private final BatchListListener listener;
		private final List<BatchRead> records;

		public ReadListCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			BatchListListener listener,
			List<BatchRead> records
		) {
			super(executor, batchPolicy, true, records.size());
			this.listener = listener;
			this.records = records;
		}

		@Override
		void writeCommand(Command command) {
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(records.size());
			command.setBatchOperate(batchPolicy, null, null, null, records, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			BatchRead record = records.get(parser.batchIndex);

			if (resultCode == 0) {
				record.setRecord(parseRecord(parser));
			}
			else {
				record.setError(resultCode, false);
			}
		}

		@Override
		void onSuccess() {
			listener.onSuccess(records);
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public static final class ReadSequenceCommand extends BaseCommand {
		private final BatchSequenceListener listener;
		private final List<BatchRead> records;

		public ReadSequenceCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			BatchSequenceListener listener,
			List<BatchRead> records
		) {
			super(executor, batchPolicy, true, records.size());
			this.listener = listener;
			this.records = records;
		}

		@Override
		void writeCommand(Command command) {
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(records.size());
			command.setBatchOperate(batchPolicy, null, null, null, records, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			BatchRead record = records.get(parser.batchIndex);

			if (resultCode == ResultCode.OK) {
				record.setRecord(parseRecord(parser));
			}
			else {
				record.setError(resultCode, false);
			}
			listener.onRecord(record);
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
	// Batch Read Key Array
	//-------------------------------------------------------

	public static final class GetArrayCommand extends BaseCommand {
		private final RecordArrayListener listener;
		private final Record[] records;
		private final Key[] keys;
		private final String[] binNames;
		private final Operation[] ops;
		private final int readAttr;
		private Exception exc;

		public GetArrayCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			RecordArrayListener listener,
			Key[] keys,
			String[] binNames,
			Operation[] ops,
			int readAttr,
			boolean isOperation
		) {
			super(executor, batchPolicy, isOperation, keys.length);
			this.listener = listener;
			this.keys = keys;
			this.binNames = binNames;
			this.ops = ops;
			this.readAttr = readAttr;
			this.records = new Record[keys.length];
		}

		@Override
		void writeCommand(Command command) {
			BatchAttr attr = new BatchAttr(batchPolicy, readAttr, ops);
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(keys.length);
			command.setBatchOperate(batchPolicy, keys, binNames, ops, attr, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			if (resultCode == ResultCode.OK) {
				records[parser.batchIndex] = parseRecord(parser);
			}
			else if (resultCode == ResultCode.INVALID_NAMESPACE) {
				exc = new AerospikeException.InvalidNamespace(keys[parser.batchIndex].namespace, 1);
			}
		}

		@Override
		void onSuccess() {
			if (exc == null) {
				listener.onSuccess(keys, records);
			}
			else {
				listener.onFailure(new AerospikeException.BatchRecords(records, exc));
			}
		}

		@Override
		void onFailure(AerospikeException ae) {
			listener.onFailure(new AerospikeException.BatchRecords(records, ae));
		}
	}

	public static final class GetSequenceCommand extends BaseCommand {
		private final RecordSequenceListener listener;
		private final Key[] keys;
		private final String[] binNames;
		private final Operation[] ops;
		private final int readAttr;

		public GetSequenceCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			RecordSequenceListener listener,
			Key[] keys,
			String[] binNames,
			Operation[] ops,
			int readAttr,
			boolean isOperation
		) {
			super(executor, batchPolicy, isOperation, keys.length);
			this.listener = listener;
			this.keys = keys;
			this.binNames = binNames;
			this.ops = ops;
			this.readAttr = readAttr;
		}

		@Override
		void writeCommand(Command command) {
			BatchAttr attr = new BatchAttr(batchPolicy, readAttr, ops);
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(keys.length);
			command.setBatchOperate(batchPolicy, keys, binNames, ops, attr, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			Key keyOrig = keys[parser.batchIndex];

			if (resultCode == ResultCode.OK) {
				Record record = parseRecord(parser);
				listener.onRecord(keyOrig, record);
			}
			else {
				listener.onRecord(keyOrig, null);
			}
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
	// Batch Exists
	//-------------------------------------------------------

	public static final class ExistsArrayCommand extends BaseCommand {
		private final ExistsArrayListener listener;
		private final Key[] keys;
		private final boolean[] existsArray;
		private Exception exc;

		public ExistsArrayCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			ExistsArrayListener listener,
			Key[] keys
		) {
			super(executor, batchPolicy, false, keys.length);
			this.listener = listener;
			this.keys = keys;
			this.existsArray = new boolean[keys.length];
		}

		@Override
		void writeCommand(Command command) {
			BatchAttr attr = new BatchAttr(batchPolicy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(keys.length);
			command.setBatchOperate(batchPolicy, keys, null, null, attr, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			if (resultCode == 0) {
				existsArray[parser.batchIndex] = true;
			}
			else if (resultCode == ResultCode.INVALID_NAMESPACE) {
				exc = new AerospikeException.InvalidNamespace(keys[parser.batchIndex].namespace, 1);
			}
		}

		@Override
		void onSuccess() {
			if (exc == null) {
				listener.onSuccess(keys, existsArray);
			}
			else {
				listener.onFailure(new AerospikeException.BatchExists(existsArray, exc));
			}
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
			super(executor, batchPolicy, false, keys.length);
			this.listener = listener;
			this.keys = keys;
		}

		@Override
		void writeCommand(Command command) {
			BatchAttr attr = new BatchAttr(batchPolicy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(keys.length);
			command.setBatchOperate(batchPolicy, keys, null, null, attr, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
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
	// Batch Operate Record List
	//-------------------------------------------------------

	public static final class OperateListCommand extends BaseCommand {
		private final AerospikeClientProxy client;
		private final BatchOperateListListener listener;
		private final List<BatchRecord> records;
		private boolean status;

		public OperateListCommand(
			AerospikeClientProxy client,
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			BatchOperateListListener listener,
			List<BatchRecord> records
		) {
			super(executor, batchPolicy, true, records.size());
			this.client = client;
			this.listener = listener;
			this.records = records;
			this.status = true;
		}

		@Override
		void writeCommand(Command command) {
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(records.size());
			command.setBatchOperate(batchPolicy, client.batchWritePolicyDefault, client.batchUDFPolicyDefault,
				client.batchDeletePolicyDefault, records, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			BatchRecord record = records.get(parser.batchIndex);

			if (resultCode == ResultCode.OK) {
				record.setRecord(parseRecord(parser));
				return;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parseRecord(parser);
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
			if (ae.getInDoubt()) {
				for (BatchRecord record : records) {
					if (record.resultCode == ResultCode.NO_RESPONSE) {
						record.inDoubt = record.hasWrite;
					}
				}
			}
			listener.onFailure(ae);
		}
	}

	public static final class OperateSequenceCommand extends BaseCommand {
		private final AerospikeClientProxy client;
		private final BatchRecordSequenceListener listener;
		private final List<BatchRecord> records;

		public OperateSequenceCommand(
			AerospikeClientProxy client,
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			BatchRecordSequenceListener listener,
			List<BatchRecord> records
		) {
			super(executor, batchPolicy, true, records.size());
			this.client = client;
			this.listener = listener;
			this.records = records;
		}

		@Override
		void writeCommand(Command command) {
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(records.size());
			command.setBatchOperate(batchPolicy, client.batchWritePolicyDefault, client.batchUDFPolicyDefault,
				client.batchDeletePolicyDefault, records, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			BatchRecord record = records.get(parser.batchIndex);

			if (resultCode == ResultCode.OK) {
				record.setRecord(parseRecord(parser));
			}
			else if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parseRecord(parser);
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
			if (ae.getInDoubt()) {
				for (BatchRecord record : records) {
					if (record.resultCode == ResultCode.NO_RESPONSE) {
						record.inDoubt = record.hasWrite;
					}
				}
			}
			listener.onFailure(ae);
		}
	}

	//-------------------------------------------------------
	// Batch Operate Key Array
	//-------------------------------------------------------

	public static final class OperateRecordArrayCommand extends BaseCommand {
		private final BatchRecordArrayListener listener;
		private final BatchRecord[] records;
		private final Key[] keys;
		private final Operation[] ops;
		private final BatchAttr attr;
		private boolean status;

		public OperateRecordArrayCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecordArrayListener listener,
			BatchAttr attr
		) {
			super(executor, batchPolicy, ops != null, keys.length);
			this.keys = keys;
			this.ops = ops;
			this.listener = listener;
			this.attr = attr;
			this.status = true;
			this.records = new BatchRecord[keys.length];

			for (int i = 0; i < keys.length; i++) {
				this.records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}
		}

		@Override
		void writeCommand(Command command) {
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(keys.length);
			command.setBatchOperate(batchPolicy, keys, null, ops, attr, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			BatchRecord record = records[parser.batchIndex];

			if (resultCode == 0) {
				record.setRecord(parseRecord(parser));
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
			if (ae.getInDoubt()) {
				for (BatchRecord record : records) {
					if (record.resultCode == ResultCode.NO_RESPONSE) {
						record.inDoubt = record.hasWrite;
					}
				}
			}
			listener.onFailure(records, ae);
		}
	}

	public static final class OperateRecordSequenceCommand extends BaseCommand {
		private final BatchRecordSequenceListener listener;
		private final Key[] keys;
		private final Operation[] ops;
		private final BatchAttr attr;

		public OperateRecordSequenceCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecordSequenceListener listener,
			BatchAttr attr
		) {
			super(executor, batchPolicy, ops != null, keys.length);
			this.keys = keys;
			this.ops = ops;
			this.listener = listener;
			this.attr = attr;
		}

		@Override
		void writeCommand(Command command) {
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(keys.length);
			command.setBatchOperate(batchPolicy, keys, null, ops, attr, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			Key keyOrig = keys[parser.batchIndex];
			BatchRecord record;

			if (resultCode == ResultCode.OK) {
				record = new BatchRecord(keyOrig, parseRecord(parser), attr.hasWrite);
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
	// Batch UDF
	//-------------------------------------------------------

	public static final class UDFArrayCommand extends BaseCommand {
		private final BatchRecordArrayListener listener;
		private final BatchRecord[] records;
		private final Key[] keys;
		private final String packageName;
		private final String functionName;
		private final byte[] argBytes;
		private final BatchAttr attr;
		private boolean status;

		public UDFArrayCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			BatchRecordArrayListener listener,
			Key[] keys,
			String packageName,
			String functionName,
			byte[] argBytes,
			BatchAttr attr
		) {
			super(executor, batchPolicy, false, keys.length);
			this.listener = listener;
			this.keys = keys;
			this.packageName = packageName;
			this.functionName = functionName;
			this.argBytes = argBytes;
			this.attr = attr;
			this.status = true;

			this.records = new BatchRecord[keys.length];

			for (int i = 0; i < keys.length; i++) {
				this.records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}
		}

		@Override
		void writeCommand(Command command) {
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(keys.length);
			command.setBatchUDF(batchPolicy, keys, packageName, functionName, argBytes, attr, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			BatchRecord record = records[parser.batchIndex];

			if (resultCode == ResultCode.OK) {
				record.setRecord(parseRecord(parser));
				return;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parseRecord(parser);
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
			if (ae.getInDoubt()) {
				for (BatchRecord record : records) {
					if (record.resultCode == ResultCode.NO_RESPONSE) {
						record.inDoubt = record.hasWrite;
					}
				}
			}
			listener.onFailure(records, ae);
		}
	}

	public static final class UDFSequenceCommand extends BaseCommand {
		private final BatchRecordSequenceListener listener;
		private final Key[] keys;
		private final String packageName;
		private final String functionName;
		private final byte[] argBytes;
		private final BatchAttr attr;

		public UDFSequenceCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			BatchRecordSequenceListener listener,
			Key[] keys,
			String packageName,
			String functionName,
			byte[] argBytes,
			BatchAttr attr
		) {
			super(executor, batchPolicy, false, keys.length);
			this.listener = listener;
			this.keys = keys;
			this.packageName = packageName;
			this.functionName = functionName;
			this.argBytes = argBytes;
			this.attr = attr;
		}

		@Override
		void writeCommand(Command command) {
			BatchOffsetsProxy offsets = new BatchOffsetsProxy(keys.length);
			command.setBatchUDF(batchPolicy, keys, packageName, functionName, argBytes, attr, offsets);
		}

		@Override
		void parse(Parser parser, int resultCode) {
			Key keyOrig = keys[parser.batchIndex];
			BatchRecord record;

			if (resultCode == ResultCode.OK) {
				record = new BatchRecord(keyOrig, parseRecord(parser), attr.hasWrite);
			}
			else if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parseRecord(parser);
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record = new BatchRecord(keyOrig, r, resultCode, inDoubt, attr.hasWrite);
				}
				else {
					record = new BatchRecord(keyOrig, null, resultCode, inDoubt, attr.hasWrite);
				}
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
	// Batch Base
	//-------------------------------------------------------

	private static abstract class BaseCommand extends CommandProxy {
		final BatchPolicy batchPolicy;
		final boolean isOperation;

		public BaseCommand(
			GrpcCallExecutor executor,
			BatchPolicy batchPolicy,
			boolean isOperation,
			int numExpectedResponses
		) {
			super(KVSGrpc.getBatchOperateStreamingMethod(), executor, batchPolicy, numExpectedResponses);
			this.batchPolicy = batchPolicy;
			this.isOperation = isOperation;
		}

		@Override
		final void onResponse(Kvs.AerospikeResponsePayload response) {
			// Check final response status for client errors (negative error codes).
			int resultCode = response.getStatus();
			boolean hasNext = response.getHasNext();

			if (resultCode != 0 && !hasNext) {
				notifyFailure(new AerospikeException(resultCode));
				return;
			}

			// Server errors are checked in response payload in Parser.
			byte[] bytes = response.getPayload().toByteArray();
			Parser parser = new Parser(bytes);
			parser.parseProto();
			int rc = parser.parseHeader();

			if (hasNext) {
				if (resultCode == 0) {
					resultCode = rc;
				}
				parser.skipKey();
				parse(parser, resultCode);
				return;
			}

			if (rc == ResultCode.OK) {
				try {
					onSuccess();
				}
				catch (Throwable t) {
					logOnSuccessError(t);
				}
			}
			else {
				notifyFailure(new AerospikeException(rc));
			}
		}

		final Record parseRecord(Parser parser) {
			return parser.parseRecord(isOperation);
		}

		abstract void parse(Parser parser, int resultCode);
		abstract void onSuccess();
	}

	//-------------------------------------------------------
	// Proxy Iterators
	//-------------------------------------------------------

	private static class BatchOffsetsProxy implements Command.BatchOffsets {
		private final int size;

		public BatchOffsetsProxy(int size) {
			this.size = size;
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public int get(int i) {
			return i;
		}
	}
}
