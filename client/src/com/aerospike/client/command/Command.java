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
package com.aerospike.client.command;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchDelete;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchUDF;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.configuration.*;
import com.aerospike.client.configuration.serializers.*;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchReadPolicy;
import com.aerospike.client.policy.BatchUDFPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryDuration;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ReadModeAP;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.BVal;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.PartitionStatus;
import com.aerospike.client.query.PartitionTracker.NodePartitions;
import com.aerospike.client.query.Statement;
import com.aerospike.client.Txn;
import com.aerospike.client.util.Packer;

public class Command {
	public static final int INFO1_READ				= (1 << 0); // Contains a read operation.
	public static final int INFO1_GET_ALL			= (1 << 1); // Get all bins.
	public static final int INFO1_SHORT_QUERY		= (1 << 2); // Short query.
	public static final int INFO1_BATCH				= (1 << 3); // Batch read or exists.
	public static final int INFO1_XDR				= (1 << 4); // Operation is being performed by XDR.
	public static final int INFO1_NOBINDATA			= (1 << 5); // Do not read the bins.
	public static final int INFO1_READ_MODE_AP_ALL	= (1 << 6); // Involve all replicas in read operation.
	public static final int INFO1_COMPRESS_RESPONSE	= (1 << 7); // Tell server to compress it's response.

	public static final int INFO2_WRITE				= (1 << 0); // Create or update record
	public static final int INFO2_DELETE			= (1 << 1); // Fling a record into the belly of Moloch.
	public static final int INFO2_GENERATION		= (1 << 2); // Update if expected generation == old.
	public static final int INFO2_GENERATION_GT		= (1 << 3); // Update if new generation >= old, good for restore.
	public static final int INFO2_DURABLE_DELETE	= (1 << 4); // Command resulting in record deletion leaves tombstone (Enterprise only).
	public static final int INFO2_CREATE_ONLY		= (1 << 5); // Create only. Fail if record already exists.
	public static final int INFO2_RELAX_AP_LONG_QUERY = (1 << 6); // Treat as long query, but relax read consistency.
	public static final int INFO2_RESPOND_ALL_OPS	= (1 << 7); // Return a result for every operation.

	public static final int INFO3_LAST				= (1 << 0); // This is the last of a multi-part message.
	public static final int INFO3_COMMIT_MASTER		= (1 << 1); // Commit to master only before declaring success.
	// On send: Do not return partition done in scan/query.
	// On receive: Specified partition is done in scan/query.
	public static final int INFO3_PARTITION_DONE	= (1 << 2);
	public static final int INFO3_UPDATE_ONLY		= (1 << 3); // Update only. Merge bins.
	public static final int INFO3_CREATE_OR_REPLACE	= (1 << 4); // Create or completely replace record.
	public static final int INFO3_REPLACE_ONLY		= (1 << 5); // Completely replace existing record only.
	public static final int INFO3_SC_READ_TYPE		= (1 << 6); // See below.
	public static final int INFO3_SC_READ_RELAX		= (1 << 7); // See below.

	// Interpret SC_READ bits in info3.
	//
	// RELAX   TYPE
	//	                strict
	//	                ------
	//   0      0     sequential (default)
	//   0      1     linearize
	//
	//	                relaxed
	//	                -------
	//   1      0     allow replica
	//   1      1     allow unavailable

	public static final int INFO4_TXN_VERIFY_READ		= (1 << 0); // Send transaction version to the server to be verified.
	public static final int INFO4_TXN_ROLL_FORWARD		= (1 << 1); // Roll forward transaction.
	public static final int INFO4_TXN_ROLL_BACK			= (1 << 2); // Roll back transaction.
	public static final int INFO4_TXN_ON_LOCKING_ONLY	= (1 << 4); // Must be able to lock record in transaction.

	public static final byte STATE_READ_AUTH_HEADER = 1;
	public static final byte STATE_READ_HEADER = 2;
	public static final byte STATE_READ_DETAIL = 3;
	public static final byte STATE_COMPLETE = 4;

	public static final byte BATCH_MSG_READ = 0x0;
	public static final byte BATCH_MSG_REPEAT = 0x1;
	public static final byte BATCH_MSG_INFO = 0x2;
	public static final byte BATCH_MSG_GEN = 0x4;
	public static final byte BATCH_MSG_TTL = 0x8;
	public static final byte BATCH_MSG_INFO4 = 0x10;

	public static final int MSG_TOTAL_HEADER_SIZE = 30;
	public static final int FIELD_HEADER_SIZE = 5;
	public static final int OPERATION_HEADER_SIZE = 8;
	public static final int MSG_REMAINING_HEADER_SIZE = 22;
	public static final int COMPRESS_THRESHOLD = 128;
	public static final long CL_MSG_VERSION = 2L;
	public static final long AS_MSG_TYPE = 3L;
	public static final long MSG_TYPE_COMPRESSED = 4L;

	public byte[] dataBuffer;
	public int dataOffset;
	public final int maxRetries;
	public final int serverTimeout;
	public int socketTimeout;
	public int totalTimeout;
	public Long version;

	public Command(int socketTimeout, int totalTimeout, int maxRetries) {
		this.maxRetries = maxRetries;
		this.totalTimeout = totalTimeout;

		if (totalTimeout > 0) {
			this.socketTimeout = (socketTimeout < totalTimeout && socketTimeout > 0)? socketTimeout : totalTimeout;
			this.serverTimeout = this.socketTimeout;
		}
		else {
			this.socketTimeout = socketTimeout;
			this.serverTimeout = 0;
		}
	}

	//--------------------------------------------------
	// Transaction
	//--------------------------------------------------

	public final void setTxnAddKeys(WritePolicy policy, Key key, OperateArgs args) {
		begin();
		int fieldCount = estimateKeySize(key);
		dataOffset += args.size;

		sizeBuffer();

		dataBuffer[8]  = MSG_REMAINING_HEADER_SIZE;
		dataBuffer[9]  = (byte)args.readAttr;
		dataBuffer[10] = (byte)args.writeAttr;
		dataBuffer[11] = (byte)0;
		dataBuffer[12] = 0;
		dataBuffer[13] = 0;
		Buffer.intToBytes(0, dataBuffer, 14);
		Buffer.intToBytes(policy.expiration, dataBuffer, 18);
		Buffer.intToBytes(serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(args.operations.length, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;

		writeKey(key);

		for (Operation operation : args.operations) {
			writeOperation(operation);
		}
		end();
		compress(policy);
	}

	public final void setTxnVerify(Key key, long ver) {
		begin();
		int fieldCount = estimateKeySize(key);

		// Version field.
		dataOffset += 7 + FIELD_HEADER_SIZE;
		fieldCount++;

		sizeBuffer();
		dataBuffer[8] = MSG_REMAINING_HEADER_SIZE;
		dataBuffer[9] = (byte)(Command.INFO1_READ | Command.INFO1_NOBINDATA);
		dataBuffer[10] = (byte)0;
		dataBuffer[11] = (byte)Command.INFO3_SC_READ_TYPE;
		dataBuffer[12] = (byte)Command.INFO4_TXN_VERIFY_READ;
		dataBuffer[13] = 0;
		Buffer.intToBytes(0, dataBuffer, 14);
		Buffer.intToBytes(0, dataBuffer, 18);
		Buffer.intToBytes(serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(0, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;

		writeKey(key);
		writeFieldVersion(ver);
		end();
	}

	public final void setBatchTxnVerify(
		BatchPolicy policy,
		Key[] keys,
		Long[] versions,
		BatchNode batch
	) {
		// Estimate buffer size.
		begin();

		// Batch field
		dataOffset += FIELD_HEADER_SIZE + 5;

		Key keyPrev = null;
		Long verPrev = null;
		int max = batch.offsetsSize;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			Key key = keys[offset];
			Long ver = versions[offset];

			dataOffset += key.digest.length + 4;

			if (canRepeat(key, keyPrev, ver, verPrev)) {
				// Can set repeat previous namespace/bin names to save space.
				dataOffset++;
			}
			else {
				// Write full header and namespace/set/bin names.
				dataOffset += 9; // header(4) + info4(1) + fieldCount(2) + opCount(2) = 9
				dataOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE;
				dataOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

				if (ver != null) {
					dataOffset += 7 + FIELD_HEADER_SIZE;
				}
				keyPrev = key;
				verPrev = ver;
			}
		}

		sizeBuffer();

		writeBatchHeader(policy, totalTimeout, 1);

		int fieldSizeOffset = dataOffset;
		writeFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

		Buffer.intToBytes(max, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = getBatchFlags(policy);
		keyPrev = null;
		verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			Key key = keys[offset];
			Long ver = versions[offset];

			Buffer.intToBytes(offset, dataBuffer, dataOffset);
			dataOffset += 4;

			byte[] digest = key.digest;
			System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
			dataOffset += digest.length;

			if (canRepeat(key, keyPrev, ver, verPrev)) {
				// Can set repeat previous namespace/bin names to save space.
				dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
			}
			else {
				// Write full message.
				dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_INFO4);
				dataBuffer[dataOffset++] = (byte)(Command.INFO1_READ | Command.INFO1_NOBINDATA);
				dataBuffer[dataOffset++] = (byte)0;
				dataBuffer[dataOffset++] = (byte)Command.INFO3_SC_READ_TYPE;
				dataBuffer[dataOffset++] = (byte)Command.INFO4_TXN_VERIFY_READ;

				int fieldCount = 0;

				if (ver != null) {
					fieldCount++;
				}

				writeBatchFields(key, fieldCount, 0);

				if (ver != null) {
					writeFieldVersion(ver);
				}

				keyPrev = key;
				verPrev = ver;
			}
		}

		// Write real field size.
		Buffer.intToBytes(dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
		end();
		compress(policy);
	}

	public final void setTxnMarkRollForward(Key key) {
		Bin bin = new Bin("fwd", true);

		begin();
		int fieldCount = estimateKeySize(key);
		estimateOperationSize(bin);
		writeTxnMonitor(key, 0, Command.INFO2_WRITE, fieldCount, 1);
		writeOperation(bin, Operation.Type.WRITE);
		end();
	}

	public final void setTxnRoll(Key key, Txn txn, int txnAttr) {
		begin();
		int fieldCount = estimateKeySize(key);

		fieldCount += sizeTxn(key, txn, false);

		sizeBuffer();
		dataBuffer[8]  = MSG_REMAINING_HEADER_SIZE;
		dataBuffer[9]  = (byte)0;
		dataBuffer[10] = (byte)Command.INFO2_WRITE | Command.INFO2_DURABLE_DELETE;
		dataBuffer[11] = (byte)0;
		dataBuffer[12] = (byte)txnAttr;
		dataBuffer[13] = 0; // clear the result code
		Buffer.intToBytes(0, dataBuffer, 14);
		Buffer.intToBytes(0, dataBuffer, 18);
		Buffer.intToBytes(serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(0, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;

		writeKey(key);
		writeTxn(txn, false);
		end();
	}

	public final void setBatchTxnRoll(
		BatchPolicy policy,
		Txn txn,
		Key[] keys,
		BatchNode batch,
		BatchAttr attr
	) {
		// Estimate buffer size.
		begin();
		int fieldCount = 1;
		int max = batch.offsetsSize;
		Long[] versions = new Long[max];

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			Key key = keys[offset];
			versions[i] = txn.getReadVersion(key);
		}

		// Batch field
		dataOffset += FIELD_HEADER_SIZE + 5;

		Key keyPrev = null;
		Long verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			Key key = keys[offset];
			Long ver = versions[i];

			dataOffset += key.digest.length + 4;

			if (canRepeat(key, keyPrev, ver, verPrev)) {
				// Can set repeat previous namespace/bin names to save space.
				dataOffset++;
			}
			else {
				// Write full header and namespace/set/bin names.
				dataOffset += 12; // header(4) + ttl(4) + fieldCount(2) + opCount(2) = 12
				dataOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE;
				dataOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
				sizeTxnBatch(txn, ver, attr.hasWrite);
				dataOffset += 2; // gen(2) = 2
				keyPrev = key;
				verPrev = ver;
			}
		}

		sizeBuffer();

		writeBatchHeader(policy, totalTimeout, fieldCount);

		int fieldSizeOffset = dataOffset;
		writeFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

		Buffer.intToBytes(max, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = getBatchFlags(policy);
		keyPrev = null;
		verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			Key key = keys[offset];
			Long ver = versions[i];

			Buffer.intToBytes(offset, dataBuffer, dataOffset);
			dataOffset += 4;

			byte[] digest = key.digest;
			System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
			dataOffset += digest.length;

			if (canRepeat(key, keyPrev, ver, verPrev)) {
				// Can set repeat previous namespace/bin names to save space.
				dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
			}
			else {
				// Write full message.
				writeBatchWrite(key, txn, ver, attr, null, 0, 0);
				keyPrev = key;
				verPrev = ver;
			}
		}

		// Write real field size.
		Buffer.intToBytes(dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
		end();
		compress(policy);
	}

	public void setTxnClose(Txn txn, Key key) {
		begin();
		int fieldCount = estimateKeySize(key);
		writeTxnMonitor(key, 0, Command.INFO2_WRITE | Command.INFO2_DELETE | Command.INFO2_DURABLE_DELETE,
			fieldCount, 0);
		end();
	}

	private void writeTxnMonitor(Key key, int readAttr, int writeAttr, int fieldCount, int opCount) {
		sizeBuffer();

		dataBuffer[8]  = MSG_REMAINING_HEADER_SIZE;
		dataBuffer[9]  = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		dataBuffer[11] = (byte)0;
		dataBuffer[12] = 0;
		dataBuffer[13] = 0;
		Buffer.intToBytes(0, dataBuffer, 14);
		Buffer.intToBytes(0, dataBuffer, 18);
		Buffer.intToBytes(serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(opCount, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;

		writeKey(key);
	}

	//--------------------------------------------------
	// Writes
	//--------------------------------------------------

	public final void setWrite(WritePolicy policy, Operation.Type operation, Key key, Bin[] bins) {
		begin();
		int fieldCount = estimateKeySize(policy, key, true);

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}

		for (Bin bin : bins) {
			estimateOperationSize(bin);
		}
		sizeBuffer();
		writeHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, bins.length);
		writeKey(policy, key, true);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}

		for (Bin bin : bins) {
			writeOperation(bin, operation);
		}
		end();
		compress(policy);
	}

	public void setDelete(WritePolicy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key, true);

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}
		sizeBuffer();
		writeHeaderWrite(policy, Command.INFO2_WRITE | Command.INFO2_DELETE, fieldCount, 0);
		writeKey(policy, key, true);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}
		end();
	}

	public void setDelete(Policy policy, Key key, BatchAttr attr) {
		begin();
		Expression exp = getBatchExpression(policy, attr);
		int fieldCount = estimateKeyAttrSize(policy, key, attr, exp);
		sizeBuffer();
		writeKeyAttr(policy, key, attr, exp, fieldCount, 0);
		end();
	}

	public final void setTouch(WritePolicy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key, true);

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}
		estimateOperationSize();
		sizeBuffer();
		writeHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, 1);
		writeKey(policy, key, true);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}
		writeOperation(Operation.Type.TOUCH);
		end();
	}

	//--------------------------------------------------
	// Reads
	//--------------------------------------------------

	public final void setExists(Policy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key, false);

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}
		sizeBuffer();
		writeHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
		writeKey(policy, key, false);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}
		end();
	}

	public final void setRead(Policy policy, Key key, String[] binNames) {
		int readAttr = Command.INFO1_READ;
		int opCount = 0;

		if (binNames != null && binNames.length > 0) {
			opCount = binNames.length;
		}
		else {
			readAttr |= Command.INFO1_GET_ALL;
		}

		begin();
		int fieldCount = estimateKeySize(policy, key, false);

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}

		if (opCount != 0) {
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}
		}

		sizeBuffer();
		writeHeaderRead(policy, serverTimeout, readAttr, 0, 0, fieldCount, opCount);
		writeKey(policy, key, false);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}

		if (opCount != 0) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		end();
	}

	public final void setRead(Policy policy, BatchRead br) {
		begin();

		BatchReadPolicy rp = br.policy;
		BatchAttr attr = new BatchAttr();
		Expression exp;
		int opCount;

		if (rp != null) {
			attr.setRead(rp);
			exp = (rp.filterExp != null) ? rp.filterExp : policy.filterExp;
		}
		else {
			attr.setRead(policy);
			exp = policy.filterExp;
		}

		if (br.binNames != null) {
			opCount = br.binNames.length;

			for (String binName : br.binNames) {
				estimateOperationSize(binName);
			}
		}
		else if (br.ops != null) {
			attr.adjustRead(br.ops);
			opCount = br.ops.length;

			for (Operation op : br.ops) {
				if (op.type.isWrite) {
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in read");
				}
				estimateOperationSize(op);
			}
		}
		else {
			attr.adjustRead(br.readAllBins);
			opCount = 0;
		}

		int fieldCount = estimateKeyAttrSize(policy, br.key, attr, exp);

		sizeBuffer();
		writeKeyAttr(policy, br.key, attr, exp, fieldCount, opCount);

		if (br.binNames != null) {
			for (String binName : br.binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		else if (br.ops != null) {
			for (Operation op : br.ops) {
				writeOperation(op);
			}
		}
		end();
	}

	public final void setRead(Policy policy, Key key, Operation[] ops) {
		begin();

		BatchAttr attr = new BatchAttr();
		attr.setRead(policy);
		attr.adjustRead(ops);

		int fieldCount = estimateKeyAttrSize(policy, key, attr, policy.filterExp);

		for (Operation op : ops) {
			if (op.type.isWrite) {
                throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in read");
			}
			estimateOperationSize(op);
		}

		sizeBuffer();
		writeKeyAttr(policy, key, attr, policy.filterExp, fieldCount, ops.length);

		for (Operation op : ops) {
			writeOperation(op);
		}
		end();
	}

	public final void setReadHeader(Policy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key, false);

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}
		sizeBuffer();
		writeHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
		writeKey(policy, key, false);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}
		end();
	}

	//--------------------------------------------------
	// Operate
	//--------------------------------------------------

	public final void setOperate(WritePolicy policy, Key key, OperateArgs args) {
		begin();
		int fieldCount = estimateKeySize(policy, key, args.hasWrite);

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}
		dataOffset += args.size;
		sizeBuffer();

		writeHeaderReadWrite(policy, args, fieldCount);
		writeKey(policy, key, args.hasWrite);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}

		for (Operation operation : args.operations) {
			writeOperation(operation);
		}
		end();
		compress(policy);
	}

	public final void setOperate(Policy policy, BatchAttr attr, Key key, Operation[] ops) {
		begin();
		Expression exp = getBatchExpression(policy, attr);
		int fieldCount = estimateKeyAttrSize(policy, key, attr, exp);

		dataOffset += attr.opSize;
		sizeBuffer();
		writeKeyAttr(policy, key, attr, exp, fieldCount, ops.length);

		for (Operation op : ops) {
			writeOperation(op);
		}
		end();
		compress(policy);
	}

	//--------------------------------------------------
	// UDF
	//--------------------------------------------------

	public final void setUdf(WritePolicy policy, Key key, String packageName, String functionName, Value[] args) {
		begin();
		int fieldCount = estimateKeySize(policy, key, true);

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}

		byte[] argBytes = Packer.pack(args);
		fieldCount += estimateUdfSize(packageName, functionName, argBytes);

		sizeBuffer();
		writeHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, 0);
		writeKey(policy, key, true);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}

		writeField(packageName, FieldType.UDF_PACKAGE_NAME);
		writeField(functionName, FieldType.UDF_FUNCTION);
		writeField(argBytes, FieldType.UDF_ARGLIST);
		end();
		compress(policy);
	}

	public final void setUdf(Policy policy, BatchAttr attr, Key key, String packageName, String functionName, Value[] args) {
		byte[] argBytes = Packer.pack(args);
		setUdf(policy, attr, key, packageName, functionName, argBytes);
	}

	public final void setUdf(Policy policy, BatchAttr attr, Key key, String packageName, String functionName, byte[] argBytes) {
		begin();
		Expression exp = getBatchExpression(policy, attr);
		int fieldCount = estimateKeyAttrSize(policy, key, attr, exp);
		fieldCount += estimateUdfSize(packageName, functionName, argBytes);

		sizeBuffer();
		writeKeyAttr(policy, key, attr, exp, fieldCount, 0);
		writeField(packageName, FieldType.UDF_PACKAGE_NAME);
		writeField(functionName, FieldType.UDF_FUNCTION);
		writeField(argBytes, FieldType.UDF_ARGLIST);
		end();
		compress(policy);
	}

	//--------------------------------------------------
	// Batch Read Only
	//--------------------------------------------------

	public final void setBatchRead(BatchPolicy policy, List<BatchRead> records, BatchNode batch) {
		// Estimate full row size
		final int[] offsets = batch.offsets;
		final int max = batch.offsetsSize;
		BatchRead prev = null;

		begin();
		int fieldCount = 1;

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}

		dataOffset += FIELD_HEADER_SIZE + 5;

		for (int i = 0; i < max; i++) {
			final BatchRead record = records.get(offsets[i]);
			final Key key = record.key;
			final String[] binNames = record.binNames;
			final Operation[] ops = record.ops;

			dataOffset += key.digest.length + 4;

			// Avoid relatively expensive full equality checks for performance reasons.
			// Use reference equality only in hope that common namespaces/bin names are set from
			// fixed variables.  It's fine if equality not determined correctly because it just
			// results in more space used. The batch will still be correct.
			if (prev != null && prev.key.namespace == key.namespace && prev.key.setName == key.setName &&
				prev.binNames == binNames && prev.readAllBins == record.readAllBins &&
				prev.ops == ops) {
				// Can set repeat previous namespace/bin names to save space.
				dataOffset++;
			}
			else {
				// Estimate full header, namespace and bin names.
				dataOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE + 6;
				dataOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

				if (binNames != null) {
					for (String binName : binNames) {
						estimateOperationSize(binName);
					}
				}
				else if (ops != null) {
					for (Operation op : ops) {
						estimateReadOperationSize(op);
					}
				}
				prev = record;
			}
		}
		sizeBuffer();

		int readAttr = Command.INFO1_READ;

		if (policy.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		writeHeaderRead(policy, totalTimeout, readAttr | Command.INFO1_BATCH, 0, 0, fieldCount, 0);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}

		final int fieldSizeOffset = dataOffset;
		writeFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

		Buffer.intToBytes(max, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (policy.allowInline)? (byte)1 : (byte)0;
		prev = null;

		for (int i = 0; i < max; i++) {
			final int index = offsets[i];
			Buffer.intToBytes(index, dataBuffer, dataOffset);
			dataOffset += 4;

			final BatchRead record = records.get(index);
			final Key key = record.key;
			final String[] binNames = record.binNames;
			final Operation[] ops = record.ops;
			final byte[] digest = key.digest;
			System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
			dataOffset += digest.length;

			// Avoid relatively expensive full equality checks for performance reasons.
			// Use reference equality only in hope that common namespaces/bin names are set from
			// fixed variables.  It's fine if equality not determined correctly because it just
			// results in more space used. The batch will still be correct.
			if (prev != null && prev.key.namespace == key.namespace && prev.key.setName == key.setName &&
				prev.binNames == binNames && prev.readAllBins == record.readAllBins &&
				prev.ops == ops) {
				// Can set repeat previous namespace/bin names to save space.
				dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
			}
			else {
				// Write full header, namespace and bin names.
				dataBuffer[dataOffset++] = BATCH_MSG_READ;

				if (binNames != null && binNames.length != 0) {
					dataBuffer[dataOffset++] = (byte)readAttr;
					writeBatchFields(key, 0, binNames.length);

					for (String binName : binNames) {
						writeOperation(binName, Operation.Type.READ);
					}
				}
				else if (ops != null) {
					int offset = dataOffset++;
					writeBatchFields(key, 0, ops.length);
					dataBuffer[offset] = (byte)writeReadOnlyOperations(ops, readAttr);
				}
				else {
					dataBuffer[dataOffset++] = (byte)(readAttr | (record.readAllBins?  Command.INFO1_GET_ALL : Command.INFO1_NOBINDATA));
					writeBatchFields(key, 0, 0);
				}
				prev = record;
			}
		}

		// Write real field size.
		Buffer.intToBytes(dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
		end();
		compress(policy);
	}

	public final void setBatchRead(
		BatchPolicy policy,
		Key[] keys,
		BatchNode batch,
		String[] binNames,
		Operation[] ops,
		int readAttr
	) {
		// Estimate full row size
		final int[] offsets = batch.offsets;
		final int max = batch.offsetsSize;

		// Estimate buffer size.
		begin();
		int fieldCount = 1;

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}

		dataOffset += FIELD_HEADER_SIZE + 5;

		Key prev = null;

		for (int i = 0; i < max; i++) {
			Key key = keys[offsets[i]];

			dataOffset += key.digest.length + 4;

			// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
			if (prev != null && prev.namespace == key.namespace && prev.setName == key.setName) {
				// Can set repeat previous namespace/bin names to save space.
				dataOffset++;
			}
			else {
				// Must write full header and namespace/set/bin names.
				dataOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE + 6;
				dataOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

				if (binNames != null) {
					for (String binName : binNames) {
						estimateOperationSize(binName);
					}
				}
				else if (ops != null) {
					for (Operation op : ops) {
						estimateReadOperationSize(op);
					}
				}
				prev = key;
			}
		}

		sizeBuffer();

		if (policy.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		writeHeaderRead(policy, totalTimeout, readAttr | Command.INFO1_BATCH, 0, 0, fieldCount, 0);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}

		int fieldSizeOffset = dataOffset;
		writeFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

		Buffer.intToBytes(max, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (policy.allowInline)? (byte)1 : (byte)0;

		prev = null;

		for (int i = 0; i < max; i++) {
			int index = offsets[i];
			Buffer.intToBytes(index, dataBuffer, dataOffset);
			dataOffset += 4;

			Key key = keys[index];
			byte[] digest = key.digest;
			System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
			dataOffset += digest.length;

			// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
			if (prev != null && prev.namespace == key.namespace && prev.setName == key.setName) {
				// Can set repeat previous namespace/bin names to save space.
				dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
			}
			else {
				// Write full header, namespace and bin names.
				dataBuffer[dataOffset++] = BATCH_MSG_READ;

				if (binNames != null && binNames.length != 0) {
					dataBuffer[dataOffset++] = (byte)readAttr;
					writeBatchFields(key, 0, binNames.length);

					for (String binName : binNames) {
						writeOperation(binName, Operation.Type.READ);
					}
				}
				else if (ops != null) {
					int offset = dataOffset++;
					writeBatchFields(key, 0, ops.length);
					dataBuffer[offset] = (byte)writeReadOnlyOperations(ops, readAttr);
				}
				else {
					dataBuffer[dataOffset++] = (byte)readAttr;
					writeBatchFields(key, 0, 0);
				}
				prev = key;
			}
		}

		// Write real field size.
		Buffer.intToBytes(dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
		end();
		compress(policy);
	}

	//--------------------------------------------------
	// Batch Read/Write Operations
	//--------------------------------------------------

	public final void setBatchOperate(
		BatchPolicy policy,
		BatchWritePolicy writePolicy,
		BatchUDFPolicy udfPolicy,
		BatchDeletePolicy deletePolicy,
		List<? extends BatchRecord> records,
		BatchNode batch,
		ConfigurationProvider configProvider
	) {
		begin();
		int max = batch.offsetsSize;
		Txn txn = policy.txn;
		Long[] versions = null;

		if (txn != null) {
			versions = new Long[max];

			for (int i = 0; i < max; i++) {
				int offset = batch.offsets[i];
				BatchRecord record = records.get(offset);
				versions[i] = txn.getReadVersion(record.key);
			}
		}

		int fieldCount = 1;

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}

		dataOffset += FIELD_HEADER_SIZE + 5;

		BatchRecord prev = null;
		Long verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			BatchRecord record = records.get(offset);
			Key key = record.key;
			Long ver = (versions != null)? versions[i] : null;

			dataOffset += key.digest.length + 4;

			if (canRepeat(policy, key, record, prev, ver, verPrev, configProvider)) {
				// Can set repeat previous namespace/bin names to save space.
				dataOffset++;
			}
			else {
				// Estimate full header, namespace and bin names.
				dataOffset += 12;
				dataOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE;
				dataOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
				sizeTxnBatch(txn, ver, record.hasWrite);
				dataOffset += record.size(policy, configProvider);
				prev = record;
				verPrev = ver;
			}
		}
		sizeBuffer();

		writeBatchHeader(policy, totalTimeout, fieldCount);

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}

		final int fieldSizeOffset = dataOffset;
		writeFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

		Buffer.intToBytes(max, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = getBatchFlags(policy);

		BatchAttr attr = new BatchAttr();
		prev = null;
		verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			BatchRecord record = records.get(offset);
			Long ver = (versions != null)? versions[i] : null;

			Buffer.intToBytes(offset, dataBuffer, dataOffset);
			dataOffset += 4;

			Key key = record.key;
			final byte[] digest = key.digest;
			System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
			dataOffset += digest.length;

			if (canRepeat(policy, key, record, prev, ver, verPrev, configProvider)) {
				// Can set repeat previous namespace/bin names to save space.
				dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
			}
			else {
				// Write full message.
				switch (record.getType()) {
					case BATCH_READ: {
						BatchRead br = (BatchRead)record;

						if (br.policy != null) {
							attr.setRead(br.policy);
						}
						else {
							attr.setRead(policy);
						}

						if (br.binNames != null) {
							if (br.binNames.length > 0) {
								writeBatchBinNames(key, txn, ver, br.binNames, attr, attr.filterExp);
							}
							else {
								attr.adjustRead(true);
								writeBatchRead(key, txn, ver, attr, attr.filterExp, 0);
							}
						}
						else if (br.ops != null) {
							attr.adjustRead(br.ops);
							writeBatchOperations(key, txn, ver, br.ops, attr, attr.filterExp);
						}
						else {
							attr.adjustRead(br.readAllBins);
							writeBatchRead(key, txn, ver, attr, attr.filterExp, 0);
						}
						break;
					}

					case BATCH_WRITE: {
						BatchWrite bw = (BatchWrite)record;
						BatchWritePolicy bwp = (bw.policy != null)? bw.policy : writePolicy;

						if (configProvider != null) {
							Configuration config = configProvider.fetchConfiguration();
							if (config != null) {
								bwp.sendKey = (config.dynamicConfiguration.dynamicBatchWriteConfig.sendKey != null) ?
										config.dynamicConfiguration.dynamicBatchWriteConfig.sendKey.value : bwp.sendKey;
								bwp.durableDelete = (config.dynamicConfiguration.dynamicBatchWriteConfig.durableDelete !=
										null) ? config.dynamicConfiguration.dynamicBatchWriteConfig.durableDelete.value :
										bwp.durableDelete;
							}
						}

						attr.setWrite(bwp);
						attr.adjustWrite(bw.ops);
						writeBatchOperations(key, txn, ver, bw.ops, attr, attr.filterExp);
						break;
					}

					case BATCH_UDF: {
						BatchUDF bu = (BatchUDF)record;
						BatchUDFPolicy bup = (bu.policy != null)? bu.policy : udfPolicy;
						if (configProvider != null) {
							Configuration config = configProvider.fetchConfiguration();
							if (config != null) {
								bup.sendKey = (config.dynamicConfiguration.dynamicBatchUDFconfig.sendKey != null) ?
										config.dynamicConfiguration.dynamicBatchUDFconfig.sendKey.value : bup.sendKey;
								bup.durableDelete = (config.dynamicConfiguration.dynamicBatchUDFconfig.durableDelete !=
										null) ? config.dynamicConfiguration.dynamicBatchUDFconfig.durableDelete.value :
										bup.durableDelete;
							}
						}

						attr.setUDF(bup);
						writeBatchWrite(key, txn, ver, attr, attr.filterExp, 3, 0);
						writeField(bu.packageName, FieldType.UDF_PACKAGE_NAME);
						writeField(bu.functionName, FieldType.UDF_FUNCTION);
						writeField(bu.argBytes, FieldType.UDF_ARGLIST);
						break;
					}

					case BATCH_DELETE: {
						BatchDelete bd = (BatchDelete)record;
						BatchDeletePolicy bdp = (bd.policy != null)? bd.policy : deletePolicy;
						if (configProvider != null) {
							Configuration config = configProvider.fetchConfiguration();
							if (config != null) {
								bdp.sendKey = (config.dynamicConfiguration.dynamicBatchDeleteConfig.sendKey != null) ?
										config.dynamicConfiguration.dynamicBatchDeleteConfig.sendKey.value : bdp.sendKey;
								bdp.durableDelete = (config.dynamicConfiguration.dynamicBatchDeleteConfig.durableDelete !=
										null) ? config.dynamicConfiguration.dynamicBatchDeleteConfig.durableDelete.value :
										bdp.durableDelete;
							}
						}

						attr.setDelete(bdp);
						writeBatchWrite(key, txn, ver, attr, attr.filterExp, 0, 0);
						break;
					}
				}
				prev = record;
				verPrev = ver;
			}
		}

		// Write real field size.
		Buffer.intToBytes(dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
		end();
		compress(policy);
	}

	public final void setBatchOperate(
		BatchPolicy policy,
		Key[] keys,
		BatchNode batch,
		String[] binNames,
		Operation[] ops,
		BatchAttr attr
	) {
		// Estimate buffer size.
		begin();
		int max = batch.offsetsSize;
		Txn txn = policy.txn;
		Long[] versions = null;

		if (txn != null) {
			versions = new Long[max];

			for (int i = 0; i < max; i++) {
				int offset = batch.offsets[i];
				Key key = keys[offset];
				versions[i] = txn.getReadVersion(key);
			}
		}

		Expression exp = getBatchExpression(policy, attr);
		int fieldCount = 1;

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}

		dataOffset += FIELD_HEADER_SIZE + 5;

		Key keyPrev = null;
		Long verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			Key key = keys[offset];
			Long ver = (versions != null)? versions[i] : null;

			dataOffset += key.digest.length + 4;

			if (canRepeat(attr, key, keyPrev, ver, verPrev)) {
				// Can set repeat previous namespace/bin names to save space.
				dataOffset++;
			}
			else {
				// Write full header and namespace/set/bin names.
				dataOffset += 12; // header(4) + ttl(4) + fieldCount(2) + opCount(2) = 12
				dataOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE;
				dataOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
				sizeTxnBatch(txn, ver, attr.hasWrite);

				if (attr.sendKey) {
					dataOffset += key.userKey.estimateSize() + FIELD_HEADER_SIZE + 1;
				}

				if (binNames != null) {
					for (String binName : binNames) {
						estimateOperationSize(binName);
					}
				}
				else if (ops != null) {
					for (Operation op : ops) {
						if (op.type.isWrite) {
							if (!attr.hasWrite) {
								throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
							}
							dataOffset += 2; // Extra write specific fields.
						}
						estimateOperationSize(op);
					}
				}
				else if ((attr.writeAttr & Command.INFO2_DELETE) != 0) {
					dataOffset += 2; // Extra write specific fields.
				}
				keyPrev = key;
				verPrev = ver;
			}
		}

		sizeBuffer();

		writeBatchHeader(policy, totalTimeout, fieldCount);

		if (exp != null) {
			exp.write(this);
		}

		int fieldSizeOffset = dataOffset;
		writeFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

		Buffer.intToBytes(max, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = getBatchFlags(policy);
		keyPrev = null;
		verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			Key key = keys[offset];
			Long ver = (versions != null)? versions[i] : null;

			Buffer.intToBytes(offset, dataBuffer, dataOffset);
			dataOffset += 4;

			byte[] digest = key.digest;
			System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
			dataOffset += digest.length;

			if (canRepeat(attr, key, keyPrev, ver, verPrev)) {
				// Can set repeat previous namespace/bin names to save space.
				dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
			}
			else {
				// Write full message.
				if (binNames != null) {
					writeBatchBinNames(key, txn, ver, binNames, attr, null);
				}
				else if (ops != null) {
					writeBatchOperations(key, txn, ver, ops, attr, null);
				}
				else if ((attr.writeAttr & Command.INFO2_DELETE) != 0) {
					writeBatchWrite(key, txn, ver, attr, null, 0, 0);
				}
				else {
					writeBatchRead(key, txn, ver, attr, null, 0);
				}
				keyPrev = key;
				verPrev = ver;
			}
		}

		// Write real field size.
		Buffer.intToBytes(dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
		end();
		compress(policy);
	}

	public final void setBatchUDF(
		BatchPolicy policy,
		Key[] keys,
		BatchNode batch,
		String packageName,
		String functionName,
		byte[] argBytes,
		BatchAttr attr
	) {
		// Estimate buffer size.
		begin();
		int max = batch.offsetsSize;
		Txn txn = policy.txn;
		Long[] versions = null;

		if (txn != null) {
			versions = new Long[max];

			for (int i = 0; i < max; i++) {
				int offset = batch.offsets[i];
				Key key = keys[offset];
				versions[i] = txn.getReadVersion(key);
			}
		}

		Expression exp = getBatchExpression(policy, attr);
		int fieldCount = 1;

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}

		dataOffset += FIELD_HEADER_SIZE + 5;

		Key keyPrev = null;
		Long verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			Key key = keys[offset];
			Long ver = (versions != null)? versions[i] : null;

			dataOffset += key.digest.length + 4;

			if (canRepeat(attr, key, keyPrev, ver, verPrev)) {
				// Can set repeat previous namespace/bin names to save space.
				dataOffset++;
			}
			else {
				// Write full header and namespace/set/bin names.
				dataOffset += 12; // header(4) + ttl(4) + fieldCount(2) + opCount(2) = 12
				dataOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE;
				dataOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
				sizeTxnBatch(txn, ver, attr.hasWrite);

				if (attr.sendKey) {
					dataOffset += key.userKey.estimateSize() + FIELD_HEADER_SIZE + 1;
				}
				dataOffset += 2; // gen(2) = 2
				estimateUdfSize(packageName, functionName, argBytes);
				keyPrev = key;
				verPrev = ver;
			}
		}

		sizeBuffer();

		writeBatchHeader(policy, totalTimeout, fieldCount);

		if (exp != null) {
			exp.write(this);
		}

		int fieldSizeOffset = dataOffset;
		writeFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

		Buffer.intToBytes(max, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = getBatchFlags(policy);
		keyPrev = null;
		verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			Key key = keys[offset];
			Long ver = (versions != null)? versions[i] : null;

			Buffer.intToBytes(offset, dataBuffer, dataOffset);
			dataOffset += 4;

			byte[] digest = key.digest;
			System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
			dataOffset += digest.length;

			if (canRepeat(attr, key, keyPrev, ver, verPrev)) {
				// Can set repeat previous namespace/bin names to save space.
				dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
			}
			else {
				// Write full message.
				writeBatchWrite(key, txn, ver, attr, null, 3, 0);
				writeField(packageName, FieldType.UDF_PACKAGE_NAME);
				writeField(functionName, FieldType.UDF_FUNCTION);
				writeField(argBytes, FieldType.UDF_ARGLIST);
				keyPrev = key;
				verPrev = ver;
			}
		}

		// Write real field size.
		Buffer.intToBytes(dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
		end();
		compress(policy);
	}

	private static boolean canRepeat(
		Policy policy,
		Key key,
		BatchRecord record,
		BatchRecord prev,
		Long ver,
		Long verPrev,
		ConfigurationProvider configProvider
	) {
		// Avoid relatively expensive full equality checks for performance reasons.
		// Use reference equality only in hope that common namespaces/bin names are set from
		// fixed variables.  It's fine if equality not determined correctly because it just
		// results in more space used. The batch will still be correct.
		// Same goes for ver reference equality check.

		if ( !(verPrev == ver && prev != null && prev.key.namespace == key.namespace &&
				prev.key.setName == key.setName )) {
			return false;
		}

		boolean sendkey = policy.sendKey;
		if (configProvider != null) {
			Configuration config = configProvider.fetchConfiguration();
			if (config != null && config.hasDBWCsendKey()) {
				sendkey = config.dynamicConfiguration.dynamicBatchWriteConfig.sendKey.value;
			}
		}
		if (sendkey) {
			return false;
		}

		return record.equals(prev);

	}

	private static boolean canRepeat(BatchAttr attr, Key key, Key keyPrev, Long ver, Long verPrev) {
		return !attr.sendKey && verPrev == ver && keyPrev != null && keyPrev.namespace == key.namespace &&
				keyPrev.setName == key.setName;
	}

	private static boolean canRepeat(Key key, Key keyPrev, Long ver, Long verPrev) {
		return verPrev == ver && keyPrev != null && keyPrev.namespace == key.namespace &&
				keyPrev.setName == key.setName;
	}

	private static final Expression getBatchExpression(Policy policy, BatchAttr attr) {
		return (attr.filterExp != null) ? attr.filterExp : policy.filterExp;
	}

	private static byte getBatchFlags(BatchPolicy policy) {
		byte flags = 0x8;

		if (policy.allowInline) {
			flags |= 0x1;
		}

		if (policy.allowInlineSSD) {
			flags |= 0x2;
		}

		if (policy.respondAllKeys) {
			flags |= 0x4;
		}
		return flags;
	}

	private void sizeTxnBatch(Txn txn, Long ver, boolean hasWrite) {
		if (txn != null) {
			dataOffset++; // Add info4 byte for transaction.
			dataOffset += 8 + FIELD_HEADER_SIZE;

			if (ver != null) {
				dataOffset += 7 + FIELD_HEADER_SIZE;
			}

			if (hasWrite && txn.getDeadline() != 0) {
				dataOffset += 4 + FIELD_HEADER_SIZE;
			}
		}
	}

	private void writeBatchHeader(Policy policy, int timeout, int fieldCount) {
		int readAttr = Command.INFO1_BATCH;

		if (policy.compress) {
			readAttr |= Command.INFO1_COMPRESS_RESPONSE;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9] = (byte)readAttr;
		dataBuffer[10] = (byte)0;
		dataBuffer[11] = (byte)0;

		for (int i = 12; i < 22; i++) {
			dataBuffer[i] = 0;
		}
		Buffer.intToBytes(timeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(0, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	private void writeBatchBinNames(Key key, Txn txn, Long ver, String[] binNames, BatchAttr attr, Expression filter) {
		writeBatchRead(key, txn, ver, attr, filter, binNames.length);

		for (String binName : binNames) {
			writeOperation(binName, Operation.Type.READ);
		}
	}

	private void writeBatchOperations(Key key, Txn txn, Long ver, Operation[] ops, BatchAttr attr, Expression filter) {
		if (attr.hasWrite) {
			writeBatchWrite(key, txn, ver, attr, filter, 0, ops.length);
		}
		else {
			writeBatchRead(key, txn, ver, attr, filter, ops.length);
		}

		for (Operation op : ops) {
			writeOperation(op);
		}
	}

	private void writeBatchRead(Key key, Txn txn, Long ver, BatchAttr attr, Expression filter, int opCount) {
		if (txn != null) {
			dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_INFO4 | BATCH_MSG_TTL);
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			dataBuffer[dataOffset++] = (byte)attr.txnAttr;
			Buffer.intToBytes(attr.expiration, dataBuffer, dataOffset);
			dataOffset += 4;
			writeBatchFieldsTxn(key, txn, ver, attr, filter, 0, opCount);
		}
		else {
			dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_TTL);
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			Buffer.intToBytes(attr.expiration, dataBuffer, dataOffset);
			dataOffset += 4;
			writeBatchFieldsReg(key, attr, filter, 0, opCount);
		}
	}

	private void writeBatchWrite(
		Key key,
		Txn txn,
		Long ver,
		BatchAttr attr,
		Expression filter,
		int fieldCount,
		int opCount
	) {
		if (txn != null) {
			dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_INFO4 | BATCH_MSG_GEN | BATCH_MSG_TTL);
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			dataBuffer[dataOffset++] = (byte)attr.txnAttr;
			Buffer.shortToBytes(attr.generation, dataBuffer, dataOffset);
			dataOffset += 2;
			Buffer.intToBytes(attr.expiration, dataBuffer, dataOffset);
			dataOffset += 4;
			writeBatchFieldsTxn(key, txn, ver, attr, filter, fieldCount, opCount);
		}
		else {
			dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_GEN | BATCH_MSG_TTL);
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			Buffer.shortToBytes(attr.generation, dataBuffer, dataOffset);
			dataOffset += 2;
			Buffer.intToBytes(attr.expiration, dataBuffer, dataOffset);
			dataOffset += 4;
			writeBatchFieldsReg(key, attr, filter, fieldCount, opCount);
		}
	}

	private void writeBatchFieldsTxn(
		Key key,
		Txn txn,
		Long ver,
		BatchAttr attr,
		Expression filter,
		int fieldCount,
		int opCount
	) {
		fieldCount++;

		if (ver != null) {
			fieldCount++;
		}

		if (attr.hasWrite && txn.getDeadline() != 0) {
			fieldCount++;
		}

		if (filter != null) {
			fieldCount++;
		}

		if (attr.sendKey) {
			fieldCount++;
		}

		writeBatchFields(key, fieldCount, opCount);

		writeFieldLE(txn.getId(), FieldType.TXN_ID);

		if (ver != null) {
			writeFieldVersion(ver);
		}

		if (attr.hasWrite && txn.getDeadline() != 0) {
			writeFieldLE(txn.getDeadline(), FieldType.TXN_DEADLINE);
		}

		if (filter != null) {
			filter.write(this);
		}

		if (attr.sendKey) {
			writeField(key.userKey, FieldType.KEY);
		}
	}

	private void writeBatchFieldsReg(
		Key key,
		BatchAttr attr,
		Expression filter,
		int fieldCount,
		int opCount
	) {
		if (filter != null) {
			fieldCount++;
		}

		if (attr.sendKey) {
			fieldCount++;
		}

		writeBatchFields(key, fieldCount, opCount);

		if (filter != null) {
			filter.write(this);
		}

		if (attr.sendKey) {
			writeField(key.userKey, FieldType.KEY);
		}
	}

	private void writeBatchFields(Key key, int fieldCount, int opCount) {
		fieldCount += 2;
		Buffer.shortToBytes(fieldCount, dataBuffer, dataOffset);
		dataOffset += 2;
		Buffer.shortToBytes(opCount, dataBuffer, dataOffset);
		dataOffset += 2;
		writeField(key.namespace, FieldType.NAMESPACE);
		writeField(key.setName, FieldType.TABLE);
	}

	//--------------------------------------------------
	// Scan
	//--------------------------------------------------

	public final void setScan(
		Cluster cluster,
		ScanPolicy policy,
		String namespace,
		String setName,
		String[] binNames,
		long taskId,
		NodePartitions nodePartitions
	) {
		begin();
		int fieldCount = 0;
		int partsFullSize = nodePartitions.partsFull.size() * 2;
		int partsPartialSize = nodePartitions.partsPartial.size() * 20;
		long maxRecords = nodePartitions.recordMax;

		if (namespace != null) {
			dataOffset += Buffer.estimateSizeUtf8(namespace) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (setName != null) {
			dataOffset += Buffer.estimateSizeUtf8(setName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (partsFullSize > 0) {
			dataOffset += partsFullSize + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (partsPartialSize > 0) {
			dataOffset += partsPartialSize + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (maxRecords > 0) {
			dataOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (policy.recordsPerSecond > 0) {
			dataOffset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}

		// Estimate scan timeout size.
		dataOffset += 4 + FIELD_HEADER_SIZE;
		fieldCount++;

		// Estimate taskId size.
		dataOffset += 8 + FIELD_HEADER_SIZE;
		fieldCount++;

		if (binNames != null) {
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}
		}

		sizeBuffer();
		int readAttr = Command.INFO1_READ;

		if (! policy.includeBinData) {
			readAttr |= Command.INFO1_NOBINDATA;
		}

		// Clusters that support partition queries also support not sending partition done messages.
		int operationCount = (binNames == null)? 0 : binNames.length;
		writeHeaderRead(policy, totalTimeout, readAttr, 0, Command.INFO3_PARTITION_DONE, fieldCount, operationCount);

		if (namespace != null) {
			writeField(namespace, FieldType.NAMESPACE);
		}

		if (setName != null) {
			writeField(setName, FieldType.TABLE);
		}

		if (partsFullSize > 0) {
			writeFieldHeader(partsFullSize, FieldType.PID_ARRAY);

			for (PartitionStatus part : nodePartitions.partsFull) {
				Buffer.shortToLittleBytes(part.id, dataBuffer, dataOffset);
				dataOffset += 2;
			}
		}

		if (partsPartialSize > 0) {
			writeFieldHeader(partsPartialSize, FieldType.DIGEST_ARRAY);

			for (PartitionStatus part : nodePartitions.partsPartial) {
				System.arraycopy(part.digest, 0, dataBuffer, dataOffset, 20);
				dataOffset += 20;
			}
		}

		if (maxRecords > 0) {
			writeField(maxRecords, FieldType.MAX_RECORDS);
		}

		if (policy.recordsPerSecond > 0) {
			writeField(policy.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
		}

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}

		// Write scan socket idle timeout.
		writeField(policy.socketTimeout, FieldType.SOCKET_TIMEOUT);

		// Write taskId field
		writeField(taskId, FieldType.QUERY_ID);

		if (binNames != null) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		end();
	}

	//--------------------------------------------------
	// Query
	//--------------------------------------------------

	@SuppressWarnings("deprecation")
	public final void setQuery(
		Cluster cluster,
		Policy policy,
		Statement statement,
		long taskId,
		boolean background,
		NodePartitions nodePartitions
	) {
		byte[] functionArgBuffer = null;
		int fieldCount = 0;
		int filterSize = 0;
		int binNameSize = 0;
		boolean isNew = cluster.hasPartitionQuery;

		begin();

		if (statement.getNamespace() != null) {
			dataOffset += Buffer.estimateSizeUtf8(statement.getNamespace()) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (statement.getSetName() != null) {
			dataOffset += Buffer.estimateSizeUtf8(statement.getSetName()) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Estimate recordsPerSecond field size. This field is used in new servers and not used
		// (but harmless to add) in old servers.
		if (statement.getRecordsPerSecond() > 0) {
			dataOffset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Estimate socket timeout field size. This field is used in new servers and not used
		// (but harmless to add) in old servers.
		dataOffset += 4 + FIELD_HEADER_SIZE;
		fieldCount++;

		// Estimate taskId field.
		dataOffset += 8 + FIELD_HEADER_SIZE;
		fieldCount++;

		Filter filter = statement.getFilter();
		String[] binNames = statement.getBinNames();
		byte[] packedCtx = null;
		String indexName = null;
		byte[] packedExp = null;

		if (filter != null) {
			IndexCollectionType type = filter.getCollectionType();

			// Estimate INDEX_TYPE field.
			if (type != IndexCollectionType.DEFAULT) {
				dataOffset += FIELD_HEADER_SIZE + 1;
				fieldCount++;
			}

			// Estimate INDEX_RANGE field.
			dataOffset += FIELD_HEADER_SIZE;
			filterSize++;  // num filters
			filterSize += filter.estimateSize();

			dataOffset += filterSize;
			fieldCount++;

			if (! isNew) {
				// Query bin names are specified as a field (Scan bin names are specified later as operations)
				// in old servers. Estimate size for selected bin names.
				if (binNames != null && binNames.length > 0) {
					dataOffset += FIELD_HEADER_SIZE;
					binNameSize++;  // num bin names

					for (String binName : binNames) {
						binNameSize += Buffer.estimateSizeUtf8(binName) + 1;
					}
					dataOffset += binNameSize;
					fieldCount++;
				}
			}

			packedCtx = filter.getPackedCtx();
			if (packedCtx != null) {
				dataOffset += FIELD_HEADER_SIZE + packedCtx.length;
				fieldCount++;
			}

			indexName = filter.getIndexName();
			if (indexName != null) {
				dataOffset += FIELD_HEADER_SIZE + Buffer.estimateSizeUtf8(indexName);
				fieldCount++;
			}

			packedExp = filter.getPackedExp();
			if (packedExp != null) {
				dataOffset += FIELD_HEADER_SIZE + packedExp.length;
				fieldCount++;
			}
		}

		// Estimate aggregation/background function size.
		if (statement.getFunctionName() != null) {
			dataOffset += FIELD_HEADER_SIZE + 1;  // udf type
			dataOffset += Buffer.estimateSizeUtf8(statement.getPackageName()) + FIELD_HEADER_SIZE;
			dataOffset += Buffer.estimateSizeUtf8(statement.getFunctionName()) + FIELD_HEADER_SIZE;

			if (statement.getFunctionArgs().length > 0) {
				functionArgBuffer = Packer.pack(statement.getFunctionArgs());
			}
			else {
				functionArgBuffer = new byte[0];
			}
			dataOffset += FIELD_HEADER_SIZE + functionArgBuffer.length;
			fieldCount += 4;
		}

		if (policy.filterExp != null) {
			dataOffset += policy.filterExp.size();
			fieldCount++;
		}

		long maxRecords = 0;
		int partsFullSize = 0;
		int partsPartialDigestSize = 0;
		int partsPartialBValSize = 0;

		if (nodePartitions != null) {
			partsFullSize = nodePartitions.partsFull.size() * 2;
			partsPartialDigestSize = nodePartitions.partsPartial.size() * 20;

			if (filter != null) {
				partsPartialBValSize = nodePartitions.partsPartial.size() * 8;
			}
			maxRecords = nodePartitions.recordMax;
		}

		if (partsFullSize > 0) {
			dataOffset += partsFullSize + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (partsPartialDigestSize > 0) {
			dataOffset += partsPartialDigestSize + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (partsPartialBValSize > 0) {
			dataOffset += partsPartialBValSize + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Estimate max records field size. This field is used in new servers and not used
		// (but harmless to add) in old servers.
		if (maxRecords > 0) {
			dataOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Operations (used in query execute) and bin names (used in scan/query) are mutually exclusive.
		Operation[] operations = statement.getOperations();
		int operationCount = 0;

		if (operations != null) {
			// Estimate size for background operations.
			if (! background) {
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Operations not allowed in foreground query");
			}

			for (Operation operation : operations) {
				if (! operation.type.isWrite) {
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Read operations not allowed in background query");
				}
				estimateOperationSize(operation);
			}
			operationCount = operations.length;
		}
		else if (binNames != null && (isNew || filter == null)) {
			// Estimate size for selected bin names (query bin names already handled for old servers).
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}
			operationCount = binNames.length;
		}

		sizeBuffer();

		if (background) {
			writeHeaderWrite((WritePolicy)policy, Command.INFO2_WRITE, fieldCount, operationCount);
		}
		else {
			QueryPolicy qp = (QueryPolicy)policy;
			int readAttr = Command.INFO1_READ;
			int writeAttr = 0;

			if (!qp.includeBinData) {
				readAttr |= Command.INFO1_NOBINDATA;
			}

			if (qp.shortQuery || qp.expectedDuration == QueryDuration.SHORT) {
				readAttr |= Command.INFO1_SHORT_QUERY;
			}
			else if (qp.expectedDuration == QueryDuration.LONG_RELAX_AP) {
				writeAttr |= Command.INFO2_RELAX_AP_LONG_QUERY;
			}

			int infoAttr = (isNew || filter == null)? Command.INFO3_PARTITION_DONE : 0;

			writeHeaderRead(policy, totalTimeout, readAttr, writeAttr, infoAttr, fieldCount, operationCount);
		}

		if (statement.getNamespace() != null) {
			writeField(statement.getNamespace(), FieldType.NAMESPACE);
		}

		if (statement.getSetName() != null) {
			writeField(statement.getSetName(), FieldType.TABLE);
		}

		// Write records per second.
		if (statement.getRecordsPerSecond() > 0) {
			writeField(statement.getRecordsPerSecond(), FieldType.RECORDS_PER_SECOND);
		}

		// Write socket idle timeout.
		writeField(policy.socketTimeout, FieldType.SOCKET_TIMEOUT);

		// Write taskId field
		writeField(taskId, FieldType.QUERY_ID);

		if (filter != null) {
			IndexCollectionType type = filter.getCollectionType();

			if (type != IndexCollectionType.DEFAULT) {
				writeFieldHeader(1, FieldType.INDEX_TYPE);
				dataBuffer[dataOffset++] = (byte)type.ordinal();
			}

			writeFieldHeader(filterSize, FieldType.INDEX_RANGE);
			dataBuffer[dataOffset++] = (byte)1;
			dataOffset = filter.write(dataBuffer, dataOffset);

			if (!isNew) {
				// Query bin names are specified as a field (Scan bin names are specified later as operations)
				// in old servers.
				if (binNames != null && binNames.length > 0) {
					writeFieldHeader(binNameSize, FieldType.QUERY_BINLIST);
					dataBuffer[dataOffset++] = (byte)binNames.length;

					for (String binName : binNames) {
						int len = Buffer.stringToUtf8(binName, dataBuffer, dataOffset + 1);
						dataBuffer[dataOffset] = (byte)len;
						dataOffset += len + 1;
					}
				}
			}

			if (packedCtx != null) {
				writeFieldHeader(packedCtx.length, FieldType.INDEX_CONTEXT);
				System.arraycopy(packedCtx, 0, dataBuffer, dataOffset, packedCtx.length);
				dataOffset += packedCtx.length;
			}

			if (indexName != null) {
				writeField(indexName, FieldType.INDEX_NAME);
			}

			if (packedExp != null) {
				writeFieldHeader(packedExp.length, FieldType.INDEX_EXPRESSION);
				System.arraycopy(packedExp, 0, dataBuffer, dataOffset, packedExp.length);
				dataOffset += packedExp.length;
			}
		}

		if (statement.getFunctionName() != null) {
			writeFieldHeader(1, FieldType.UDF_OP);
			dataBuffer[dataOffset++] = background? (byte)2 : (byte)1;
			writeField(statement.getPackageName(), FieldType.UDF_PACKAGE_NAME);
			writeField(statement.getFunctionName(), FieldType.UDF_FUNCTION);
			writeField(functionArgBuffer, FieldType.UDF_ARGLIST);
		}

		if (policy.filterExp != null) {
			policy.filterExp.write(this);
		}

		if (partsFullSize > 0) {
			writeFieldHeader(partsFullSize, FieldType.PID_ARRAY);

			for (PartitionStatus part : nodePartitions.partsFull) {
				Buffer.shortToLittleBytes(part.id, dataBuffer, dataOffset);
				dataOffset += 2;
			}
		}

		if (partsPartialDigestSize > 0) {
			writeFieldHeader(partsPartialDigestSize, FieldType.DIGEST_ARRAY);

			for (PartitionStatus part : nodePartitions.partsPartial) {
				System.arraycopy(part.digest, 0, dataBuffer, dataOffset, 20);
				dataOffset += 20;
			}
		}

		if (partsPartialBValSize > 0) {
			writeFieldHeader(partsPartialBValSize, FieldType.BVAL_ARRAY);

			for (PartitionStatus part : nodePartitions.partsPartial) {
				Buffer.longToLittleBytes(part.bval, dataBuffer, dataOffset);
				dataOffset += 8;
			}
		}

		if (maxRecords > 0) {
			writeField(maxRecords, FieldType.MAX_RECORDS);
		}

		if (operations != null) {
			for (Operation operation : operations) {
				writeOperation(operation);
			}
		}
		else if (binNames != null && (isNew || filter == null)) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}

		end();
	}

	//--------------------------------------------------
	// Command Sizing
	//--------------------------------------------------

	private final int estimateKeyAttrSize(Policy policy, Key key, BatchAttr attr, Expression filterExp) {
		int fieldCount = estimateKeySize(policy, key, attr.hasWrite);

		if (filterExp != null) {
			dataOffset += filterExp.size();
			fieldCount++;
		}
		return fieldCount;
	}

	private int estimateKeySize(Policy policy, Key key, boolean hasWrite) {
		int fieldCount = estimateKeySize(key);

		fieldCount += sizeTxn(key, policy.txn, hasWrite);

		if (policy.sendKey) {
			dataOffset += key.userKey.estimateSize() + FIELD_HEADER_SIZE + 1;
			fieldCount++;
		}
		return fieldCount;
	}

	protected final int estimateKeySize(Key key) {
		int fieldCount = 0;

		if (key.namespace != null) {
			dataOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (key.setName != null) {
			dataOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		dataOffset += key.digest.length + FIELD_HEADER_SIZE;
		fieldCount++;
		return fieldCount;
	}

	private final int estimateUdfSize(String packageName, String functionName, byte[] bytes) {
		dataOffset += Buffer.estimateSizeUtf8(packageName) + FIELD_HEADER_SIZE;
		dataOffset += Buffer.estimateSizeUtf8(functionName) + FIELD_HEADER_SIZE;
		dataOffset += bytes.length + FIELD_HEADER_SIZE;
		return 3;
	}

	private final void estimateOperationSize(Bin bin) {
		dataOffset += Buffer.estimateSizeUtf8(bin.name) + OPERATION_HEADER_SIZE;
		dataOffset += bin.value.estimateSize();
	}

	private final void estimateOperationSize(Operation operation) {
		dataOffset += Buffer.estimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
		dataOffset += operation.value.estimateSize();
	}

	private void estimateReadOperationSize(Operation operation) {
		if (operation.type.isWrite) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
		}
		dataOffset += Buffer.estimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
		dataOffset += operation.value.estimateSize();
	}

	private final void estimateOperationSize(String binName) {
		dataOffset += Buffer.estimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
	}

	private final void estimateOperationSize() {
		dataOffset += OPERATION_HEADER_SIZE;
	}

	//--------------------------------------------------
	// Command Writes
	//--------------------------------------------------

	/**
	 * Header write for write commands.
	 */
	private final void writeHeaderWrite(WritePolicy policy, int writeAttr, int fieldCount, int operationCount) {
		// Set flags.
		int generation = 0;
		int readAttr = 0;
		int infoAttr = 0;
		int txnAttr = 0;

		switch (policy.recordExistsAction) {
		case UPDATE:
			break;
		case UPDATE_ONLY:
			infoAttr |= Command.INFO3_UPDATE_ONLY;
			break;
		case REPLACE:
			infoAttr |= Command.INFO3_CREATE_OR_REPLACE;
			break;
		case REPLACE_ONLY:
			infoAttr |= Command.INFO3_REPLACE_ONLY;
			break;
		case CREATE_ONLY:
			writeAttr |= Command.INFO2_CREATE_ONLY;
			break;
		}

		switch (policy.generationPolicy) {
		case NONE:
			break;
		case EXPECT_GEN_EQUAL:
			generation = policy.generation;
			writeAttr |= Command.INFO2_GENERATION;
			break;
		case EXPECT_GEN_GT:
			generation = policy.generation;
			writeAttr |= Command.INFO2_GENERATION_GT;
			break;
		}

		if (policy.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}

		if (policy.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (policy.onLockingOnly) {
			txnAttr |= Command.INFO4_TXN_ON_LOCKING_ONLY;
		}

		if (policy.xdr) {
			readAttr |= Command.INFO1_XDR;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8]  = MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9]  = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		dataBuffer[11] = (byte)infoAttr;
		dataBuffer[12] = (byte)txnAttr;
		dataBuffer[13] = 0; // clear the result code
		Buffer.intToBytes(generation, dataBuffer, 14);
		Buffer.intToBytes(policy.expiration, dataBuffer, 18);
		Buffer.intToBytes(serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	/**
	 * Header write for operate command.
	 */
	private final void writeHeaderReadWrite(
		WritePolicy policy,
		OperateArgs args,
		int fieldCount
	) {
		// Set flags.
		int generation = 0;
		int ttl = args.hasWrite ? policy.expiration : policy.readTouchTtlPercent;
		int readAttr = args.readAttr;
		int writeAttr = args.writeAttr;
		int infoAttr = 0;
		int txnAttr = 0;
		int operationCount = args.operations.length;

		switch (policy.recordExistsAction) {
		case UPDATE:
			break;
		case UPDATE_ONLY:
			infoAttr |= Command.INFO3_UPDATE_ONLY;
			break;
		case REPLACE:
			infoAttr |= Command.INFO3_CREATE_OR_REPLACE;
			break;
		case REPLACE_ONLY:
			infoAttr |= Command.INFO3_REPLACE_ONLY;
			break;
		case CREATE_ONLY:
			writeAttr |= Command.INFO2_CREATE_ONLY;
			break;
		}

		switch (policy.generationPolicy) {
		case NONE:
			break;
		case EXPECT_GEN_EQUAL:
			generation = policy.generation;
			writeAttr |= Command.INFO2_GENERATION;
			break;
		case EXPECT_GEN_GT:
			generation = policy.generation;
			writeAttr |= Command.INFO2_GENERATION_GT;
			break;
		}

		if (policy.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}

		if (policy.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (policy.onLockingOnly) {
			txnAttr |= Command.INFO4_TXN_ON_LOCKING_ONLY;
		}

		if (policy.xdr) {
			readAttr |= Command.INFO1_XDR;
		}

		switch (policy.readModeSC) {
		case SESSION:
			break;
		case LINEARIZE:
			infoAttr |= Command.INFO3_SC_READ_TYPE;
			break;
		case ALLOW_REPLICA:
			infoAttr |= Command.INFO3_SC_READ_RELAX;
			break;
		case ALLOW_UNAVAILABLE:
			infoAttr |= Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
			break;
		}

		if (policy.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		if (policy.compress) {
			readAttr |= Command.INFO1_COMPRESS_RESPONSE;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8]  = MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9]  = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		dataBuffer[11] = (byte)infoAttr;
		dataBuffer[12] = (byte)txnAttr;
		dataBuffer[13] = 0; // clear the result code
		Buffer.intToBytes(generation, dataBuffer, 14);
		Buffer.intToBytes(ttl, dataBuffer, 18);
		Buffer.intToBytes(serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	/**
	 * Header write for read commands.
	 */
	private final void writeHeaderRead(
		Policy policy,
		int timeout,
		int readAttr,
		int writeAttr,
		int infoAttr,
		int fieldCount,
		int operationCount
	) {
		switch (policy.readModeSC) {
		case SESSION:
			break;
		case LINEARIZE:
			infoAttr |= Command.INFO3_SC_READ_TYPE;
			break;
		case ALLOW_REPLICA:
			infoAttr |= Command.INFO3_SC_READ_RELAX;
			break;
		case ALLOW_UNAVAILABLE:
			infoAttr |= Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
			break;
		}

		if (policy.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		if (policy.compress) {
			readAttr |= Command.INFO1_COMPRESS_RESPONSE;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9] = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		dataBuffer[11] = (byte)infoAttr;

		for (int i = 12; i < 18; i++) {
			dataBuffer[i] = 0;
		}
		Buffer.intToBytes(policy.readTouchTtlPercent, dataBuffer, 18);
		Buffer.intToBytes(timeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	/**
	 * Header write for read header commands.
	 */
	private final void writeHeaderReadHeader(Policy policy, int readAttr, int fieldCount, int operationCount) {
		int infoAttr = 0;

		switch (policy.readModeSC) {
		case SESSION:
			break;
		case LINEARIZE:
			infoAttr |= Command.INFO3_SC_READ_TYPE;
			break;
		case ALLOW_REPLICA:
			infoAttr |= Command.INFO3_SC_READ_RELAX;
			break;
		case ALLOW_UNAVAILABLE:
			infoAttr |= Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
			break;
		}

		if (policy.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9] = (byte)readAttr;
		dataBuffer[10] = (byte)0;
		dataBuffer[11] = (byte)infoAttr;

		for (int i = 12; i < 18; i++) {
			dataBuffer[i] = 0;
		}
		Buffer.intToBytes(policy.readTouchTtlPercent, dataBuffer, 18);
		Buffer.intToBytes(serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	/**
	 * Header write for batch single commands.
	 */
	private void writeKeyAttr(
		Policy policy,
		Key key,
		BatchAttr attr,
		Expression filterExp,
		int fieldCount,
		int operationCount
	) {
		// Write all header data except total size which must be written last.
		dataBuffer[8]  = MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9]  = (byte)attr.readAttr;
		dataBuffer[10] = (byte)attr.writeAttr;
		dataBuffer[11] = (byte)attr.infoAttr;
		dataBuffer[12] = (byte)attr.txnAttr;
		dataBuffer[13] = 0; // clear the result code
		Buffer.intToBytes(attr.generation, dataBuffer, 14);
		Buffer.intToBytes(attr.expiration, dataBuffer, 18);
		Buffer.intToBytes(serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;

		writeKey(policy, key, attr.hasWrite);

		if (filterExp != null) {
			filterExp.write(this);
		}
	}

	private void writeKey(Policy policy, Key key, boolean sendDeadline) {
		writeKey(key);
		writeTxn(policy.txn, sendDeadline);

		if (policy.sendKey) {
			writeField(key.userKey, FieldType.KEY);
		}
	}

	protected final void writeKey(Key key) {
		// Write key into buffer.
		if (key.namespace != null) {
			writeField(key.namespace, FieldType.NAMESPACE);
		}

		if (key.setName != null) {
			writeField(key.setName, FieldType.TABLE);
		}

		writeField(key.digest, FieldType.DIGEST_RIPE);
	}

	private final int writeReadOnlyOperations(Operation[] ops, int readAttr) {
		boolean readBin = false;
		boolean readHeader = false;

		for (Operation op : ops) {
			switch (op.type) {
			case READ:
				// Read all bins if no bin is specified.
				if (op.binName == null) {
					readAttr |= Command.INFO1_GET_ALL;
				}
				readBin = true;
				break;

			case READ_HEADER:
				readHeader = true;
				break;

			default:
				break;
			}
			writeOperation(op);
		}

		if (readHeader && ! readBin) {
			readAttr |= Command.INFO1_NOBINDATA;
		}
		return readAttr;
	}

	private final void writeOperation(Bin bin, Operation.Type operation) {
		int nameLength = Buffer.stringToUtf8(bin.name, dataBuffer, dataOffset + OPERATION_HEADER_SIZE);
		int valueLength = bin.value.write(dataBuffer, dataOffset + OPERATION_HEADER_SIZE + nameLength);

		Buffer.intToBytes(nameLength + valueLength + 4, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (byte) operation.protocolType;
		dataBuffer[dataOffset++] = (byte) bin.value.getType();
		dataBuffer[dataOffset++] = (byte) 0;
		dataBuffer[dataOffset++] = (byte) nameLength;
		dataOffset += nameLength + valueLength;
	}

	private final void writeOperation(Operation operation) {
		int nameLength = Buffer.stringToUtf8(operation.binName, dataBuffer, dataOffset + OPERATION_HEADER_SIZE);
		int valueLength = operation.value.write(dataBuffer, dataOffset + OPERATION_HEADER_SIZE + nameLength);

		Buffer.intToBytes(nameLength + valueLength + 4, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (byte) operation.type.protocolType;
		dataBuffer[dataOffset++] = (byte) operation.value.getType();
		dataBuffer[dataOffset++] = (byte) 0;
		dataBuffer[dataOffset++] = (byte) nameLength;
		dataOffset += nameLength + valueLength;
	}

	private final void writeOperation(String name, Operation.Type operation) {
		int nameLength = Buffer.stringToUtf8(name, dataBuffer, dataOffset + OPERATION_HEADER_SIZE);

		Buffer.intToBytes(nameLength + 4, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (byte) operation.protocolType;
		dataBuffer[dataOffset++] = (byte) 0;
		dataBuffer[dataOffset++] = (byte) 0;
		dataBuffer[dataOffset++] = (byte) nameLength;
		dataOffset += nameLength;
	}

	private final void writeOperation(Operation.Type operation) {
		Buffer.intToBytes(4, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (byte) operation.protocolType;
		dataBuffer[dataOffset++] = 0;
		dataBuffer[dataOffset++] = 0;
		dataBuffer[dataOffset++] = 0;
	}

	private int sizeTxn(Key key, Txn txn, boolean hasWrite) {
		int fieldCount = 0;

		if (txn != null) {
			dataOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;

			version = txn.getReadVersion(key);

			if (version != null) {
				dataOffset += 7 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (hasWrite && txn.getDeadline() != 0) {
				dataOffset += 4 + FIELD_HEADER_SIZE;
				fieldCount++;
			}
		}
		return fieldCount;
	}

	private void writeTxn(Txn txn, boolean sendDeadline) {
		if (txn != null) {
			writeFieldLE(txn.getId(), FieldType.TXN_ID);

			if (version != null) {
				writeFieldVersion(version);
			}

			if (sendDeadline && txn.getDeadline() != 0) {
				writeFieldLE(txn.getDeadline(), FieldType.TXN_DEADLINE);
			}
		}
	}

	private void writeFieldVersion(long ver) {
		writeFieldHeader(7, FieldType.RECORD_VERSION);
		Buffer.longToVersionBytes(ver, dataBuffer, dataOffset);
		dataOffset += 7;
	}

	private void writeField(Value value, int type) {
		int offset = dataOffset + FIELD_HEADER_SIZE;
		dataBuffer[offset++] = (byte)value.getType();
		int len = value.write(dataBuffer, offset) + 1;
		writeFieldHeader(len, type);
		dataOffset += len;
	}

	private void writeField(String str, int type) {
		int len = Buffer.stringToUtf8(str, dataBuffer, dataOffset + FIELD_HEADER_SIZE);
		writeFieldHeader(len, type);
		dataOffset += len;
	}

	private void writeField(byte[] bytes, int type) {
		System.arraycopy(bytes, 0, dataBuffer, dataOffset + FIELD_HEADER_SIZE, bytes.length);
		writeFieldHeader(bytes.length, type);
		dataOffset += bytes.length;
	}

	private void writeField(int val, int type) {
		writeFieldHeader(4, type);
		Buffer.intToBytes(val, dataBuffer, dataOffset);
		dataOffset += 4;
	}

	private void writeFieldLE(int val, int type) {
		writeFieldHeader(4, type);
		Buffer.intToLittleBytes(val, dataBuffer, dataOffset);
		dataOffset += 4;
	}

	private void writeField(long val, int type) {
		writeFieldHeader(8, type);
		Buffer.longToBytes(val, dataBuffer, dataOffset);
		dataOffset += 8;
	}

	private void writeFieldLE(long val, int type) {
		writeFieldHeader(8, type);
		Buffer.longToLittleBytes(val, dataBuffer, dataOffset);
		dataOffset += 8;
	}

	private void writeFieldHeader(int size, int type) {
		Buffer.intToBytes(size+1, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (byte)type;
	}

	public final void writeExpHeader(int size) {
		writeFieldHeader(size, FieldType.FILTER_EXP);
	}

	protected final void begin() {
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	protected final void end() {
		// Write total size of message which is the current offset.
		long proto = (dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
		Buffer.longToBytes(proto, dataBuffer, 0);
	}

	private final void compress(Policy policy) {
		if (policy.compress && dataOffset > COMPRESS_THRESHOLD) {
			Deflater def = new Deflater(Deflater.BEST_SPEED);
			try {
				def.setInput(dataBuffer, 0, dataOffset);
				def.finish();

				byte[] cbuf = new byte[dataOffset];
				int csize = def.deflate(cbuf, 16, dataOffset - 16);

				// Use compressed buffer if compression completed within original buffer size.
				if (def.finished()) {
					long proto = (csize + 8) | (CL_MSG_VERSION << 56) | (MSG_TYPE_COMPRESSED << 48);
					Buffer.longToBytes(proto, cbuf, 0);
					Buffer.longToBytes(dataOffset, cbuf, 8);
					dataBuffer = cbuf;
					dataOffset = csize + 16;
				}
			} finally {
				def.end();
			}
		}
	}

	protected void sizeBuffer() {
	}

	//--------------------------------------------------
	// Response Parsing
	//--------------------------------------------------

	protected final void skipKey(int fieldCount) {
		// There can be fields in the response (setname etc).
		// But for now, ignore them. Expose them to the API if needed in the future.
		for (int i = 0; i < fieldCount; i++) {
			int fieldlen = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4 + fieldlen;
		}
	}

	protected final Key parseKey(int fieldCount, BVal bval) {
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
				break;

			case FieldType.NAMESPACE:
				namespace = Buffer.utf8ToString(dataBuffer, dataOffset, size);
				break;

			case FieldType.TABLE:
				setName = Buffer.utf8ToString(dataBuffer, dataOffset, size);
				break;

			case FieldType.KEY:
				int type = dataBuffer[dataOffset++];
				size--;
				userKey = Buffer.bytesToKeyValue(type, dataBuffer, dataOffset, size);
				break;

			case FieldType.BVAL_ARRAY:
				bval.val = Buffer.littleBytesToLong(dataBuffer, dataOffset);
				break;
			}
			dataOffset += size;
		}
		return new Key(namespace, digest, setName, userKey);
	}

	public Long parseVersion(int fieldCount) {
		Long version = null;

		for (int i = 0; i < fieldCount; i++) {
			int len = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;

			int type = dataBuffer[dataOffset++];
			int size = len - 1;

			if (type == FieldType.RECORD_VERSION && size == 7) {
				version = Buffer.versionBytesToLong(dataBuffer, dataOffset);
			}
			dataOffset += size;
		}
		return version;
	}

	protected final Record parseRecord(
		int opCount,
		int generation,
		int expiration,
		boolean isOperation
	)  {
		Map<String,Object> bins = new LinkedHashMap<>();

		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(dataBuffer, dataOffset);
			byte particleType = dataBuffer[dataOffset + 5];
			byte nameSize = dataBuffer[dataOffset + 7];
			String name = Buffer.utf8ToString(dataBuffer, dataOffset + 8, nameSize);
			dataOffset += 4 + 4 + nameSize;

			int particleBytesSize = opSize - (4 + nameSize);
			Object value = Buffer.bytesToParticle(particleType, dataBuffer, dataOffset, particleBytesSize);
			dataOffset += particleBytesSize;

			if (isOperation) {
				if (bins.containsKey(name)) {
					// Multiple values returned for the same bin.
					Object prev = bins.get(name);

					if (prev instanceof OpResults) {
						// List already exists.  Add to it.
						OpResults list = (OpResults)prev;
						list.add(value);
					}
					else {
						// Make a list to store all values.
						OpResults list = new OpResults();
						list.add(prev);
						list.add(value);
						bins.put(name, list);
					}
				}
				else {
					bins.put(name, value);
				}
			}
			else {
				bins.put(name, value);
			}
		}
		return new Record(bins, generation, expiration);
	}

	public static boolean batchInDoubt(boolean isWrite, int commandSentCounter) {
		return isWrite && commandSentCounter > 1;
	}

	public static class OpResults extends ArrayList<Object> {
		private static final long serialVersionUID = 1L;
	}
}
