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
package com.aerospike.client.command;

import java.util.List;
import java.util.zip.Deflater;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.exp.CommandExp;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ReadModeAP;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.PartitionStatus;
import com.aerospike.client.query.PartitionTracker.NodePartitions;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Packer;

@SuppressWarnings("deprecation")
public abstract class Command {
	public static final int INFO1_READ				= (1 << 0); // Contains a read operation.
	public static final int INFO1_GET_ALL			= (1 << 1); // Get all bins.
	public static final int INFO1_BATCH				= (1 << 3); // Batch read or exists.
	public static final int INFO1_XDR				= (1 << 4); // Operation is being performed by XDR.
	public static final int INFO1_NOBINDATA			= (1 << 5); // Do not read the bins.
	public static final int INFO1_READ_MODE_AP_ALL	= (1 << 6); // Involve all replicas in read operation.
	public static final int INFO1_COMPRESS_RESPONSE	= (1 << 7); // Tell server to compress it's response.

	public static final int INFO2_WRITE				= (1 << 0); // Create or update record
	public static final int INFO2_DELETE			= (1 << 1); // Fling a record into the belly of Moloch.
	public static final int INFO2_GENERATION		= (1 << 2); // Update if expected generation == old.
	public static final int INFO2_GENERATION_GT		= (1 << 3); // Update if new generation >= old, good for restore.
	public static final int INFO2_DURABLE_DELETE	= (1 << 4); // Transaction resulting in record deletion leaves tombstone (Enterprise only).
	public static final int INFO2_CREATE_ONLY		= (1 << 5); // Create only. Fail if record already exists.
	public static final int INFO2_RESPOND_ALL_OPS	= (1 << 7); // Return a result for every operation.

	public static final int INFO3_LAST				= (1 << 0); // This is the last of a multi-part message.
	public static final int INFO3_COMMIT_MASTER		= (1 << 1); // Commit to master only before declaring success.
	public static final int INFO3_PARTITION_DONE	= (1 << 2); // Partition is complete response in scan.
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

	public static final byte STATE_READ_AUTH_HEADER = 1;
	public static final byte STATE_READ_HEADER = 2;
	public static final byte STATE_READ_DETAIL = 3;
	public static final byte STATE_COMPLETE = 4;

	public static final int MSG_TOTAL_HEADER_SIZE = 30;
	public static final int FIELD_HEADER_SIZE = 5;
	public static final int OPERATION_HEADER_SIZE = 8;
	public static final int MSG_REMAINING_HEADER_SIZE = 22;
	public static final int DIGEST_SIZE = 20;
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

	public final void setWrite(WritePolicy policy, Operation.Type operation, Key key, Bin[] bins) throws AerospikeException {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}

		for (Bin bin : bins) {
			estimateOperationSize(bin);
		}
		sizeBuffer();
		writeHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, bins.length);
		writeKey(policy, key);

		if (exp != null) {
			dataOffset = exp.write(this);
		}

		for (Bin bin : bins) {
			writeOperation(bin, operation);
		}
		end();
		compress(policy);
	}

	public void setDelete(WritePolicy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}
		sizeBuffer();
		writeHeaderWrite(policy, Command.INFO2_WRITE | Command.INFO2_DELETE, fieldCount, 0);
		writeKey(policy, key);

		if (exp != null) {
			dataOffset = exp.write(this);
		}
		end();
	}

	public final void setTouch(WritePolicy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}
		estimateOperationSize();
		sizeBuffer();
		writeHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, 1);
		writeKey(policy, key);

		if (exp != null) {
			dataOffset = exp.write(this);
		}
		writeOperation(Operation.Type.TOUCH);
		end();
	}

	public final void setExists(Policy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}
		sizeBuffer();
		writeHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
		writeKey(policy, key);

		if (exp != null) {
			dataOffset = exp.write(this);
		}
		end();
	}

	private final void setRead(Policy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}
		sizeBuffer();
		writeHeaderRead(policy, serverTimeout, Command.INFO1_READ | Command.INFO1_GET_ALL, fieldCount, 0);
		writeKey(policy, key);

		if (exp != null) {
			dataOffset = exp.write(this);
		}
		end();
	}

	public final void setRead(Policy policy, Key key, String[] binNames) {
		if (binNames != null) {
			begin();
			int fieldCount = estimateKeySize(policy, key);
			CommandExp exp = getCommandExp(policy);

			if (exp != null) {
				dataOffset += exp.size();
				fieldCount++;
			}

			for (String binName : binNames) {
				estimateOperationSize(binName);
			}
			sizeBuffer();
			writeHeaderRead(policy, serverTimeout, Command.INFO1_READ, fieldCount, binNames.length);
			writeKey(policy, key);

			if (exp != null) {
				dataOffset = exp.write(this);
			}

			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
			end();
		}
		else {
			setRead(policy, key);
		}
	}

	public final void setReadHeader(Policy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}
		estimateOperationSize((String)null);
		sizeBuffer();
		writeHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
		writeKey(policy, key);

		if (exp != null) {
			dataOffset = exp.write(this);
		}
		end();
	}

	public final void setOperate(WritePolicy policy, Key key, OperateArgs args) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}
		dataOffset += args.size;
		sizeBuffer();

		writeHeaderReadWrite(policy, args.readAttr, args.writeAttr, fieldCount, args.operations.length);
		writeKey(policy, key);

		if (exp != null) {
			dataOffset = exp.write(this);
		}

		for (Operation operation : args.operations) {
			writeOperation(operation);
		}
		end();
		compress(policy);
	}

	public final void setUdf(WritePolicy policy, Key key, String packageName, String functionName, Value[] args)
		throws AerospikeException {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}

		byte[] argBytes = Packer.pack(args);
		fieldCount += estimateUdfSize(packageName, functionName, argBytes);

		sizeBuffer();
		writeHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, 0);
		writeKey(policy, key);

		if (exp != null) {
			dataOffset = exp.write(this);
		}

		writeField(packageName, FieldType.UDF_PACKAGE_NAME);
		writeField(functionName, FieldType.UDF_FUNCTION);
		writeField(argBytes, FieldType.UDF_ARGLIST);
		end();
		compress(policy);
	}

	public final void setBatchRead(BatchPolicy policy, List<BatchRead> records, BatchNode batch) {
		// Estimate full row size
		final int[] offsets = batch.offsets;
		final int max = batch.offsetsSize;
		final int fieldCountRow = policy.sendSetName ? 2 : 1;
		BatchRead prev = null;

		begin();
		int fieldCount = 1;
		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}

		dataOffset += FIELD_HEADER_SIZE + 5;

		for (int i = 0; i < max; i++) {
			final BatchRead record = records.get(offsets[i]);
			final Key key = record.key;
			final String[] binNames = record.binNames;

			dataOffset += key.digest.length + 4;

			// Avoid relatively expensive full equality checks for performance reasons.
			// Use reference equality only in hope that common namespaces/bin names are set from
			// fixed variables.  It's fine if equality not determined correctly because it just
			// results in more space used. The batch will still be correct.
			if (prev != null && prev.key.namespace == key.namespace &&
				(! policy.sendSetName || prev.key.setName == key.setName) &&
				prev.binNames == binNames && prev.readAllBins == record.readAllBins) {
				// Can set repeat previous namespace/bin names to save space.
				dataOffset++;
			}
			else {
				// Estimate full header, namespace and bin names.
				dataOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE + 6;

				if (policy.sendSetName) {
					dataOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
				}

				if (binNames != null) {
					for (String binName : binNames) {
						estimateOperationSize(binName);
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

		writeHeaderRead(policy, totalTimeout, readAttr | Command.INFO1_BATCH, fieldCount, 0);

		if (exp != null) {
			dataOffset = exp.write(this);
		}

		final int fieldSizeOffset = dataOffset;
		writeFieldHeader(0, policy.sendSetName? FieldType.BATCH_INDEX_WITH_SET : FieldType.BATCH_INDEX);  // Need to update size at end

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
			final byte[] digest = key.digest;
			System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
			dataOffset += digest.length;

			// Avoid relatively expensive full equality checks for performance reasons.
			// Use reference equality only in hope that common namespaces/bin names are set from
			// fixed variables.  It's fine if equality not determined correctly because it just
			// results in more space used. The batch will still be correct.
			if (prev != null && prev.key.namespace == key.namespace &&
				(! policy.sendSetName || prev.key.setName == key.setName) &&
				prev.binNames == binNames && prev.readAllBins == record.readAllBins) {
				// Can set repeat previous namespace/bin names to save space.
				dataBuffer[dataOffset++] = 1;  // repeat
			}
			else {
				// Write full header, namespace and bin names.
				dataBuffer[dataOffset++] = 0;  // do not repeat

				if (binNames != null && binNames.length != 0) {
					dataBuffer[dataOffset++] = (byte)readAttr;
					Buffer.shortToBytes(fieldCountRow, dataBuffer, dataOffset);
					dataOffset += 2;
					Buffer.shortToBytes(binNames.length, dataBuffer, dataOffset);
					dataOffset += 2;
					writeField(key.namespace, FieldType.NAMESPACE);

					if (policy.sendSetName) {
						writeField(key.setName, FieldType.TABLE);
					}

					for (String binName : binNames) {
						writeOperation(binName, Operation.Type.READ);
					}
				}
				else {
					dataBuffer[dataOffset++] = (byte)(readAttr | (record.readAllBins?  Command.INFO1_GET_ALL : Command.INFO1_NOBINDATA));
					Buffer.shortToBytes(fieldCountRow, dataBuffer, dataOffset);
					dataOffset += 2;
					Buffer.shortToBytes(0, dataBuffer, dataOffset);
					dataOffset += 2;
					writeField(key.namespace, FieldType.NAMESPACE);

					if (policy.sendSetName) {
						writeField(key.setName, FieldType.TABLE);
					}
				}
				prev = record;
			}
		}

		// Write real field size.
		Buffer.intToBytes(dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
		end();
		compress(policy);
	}

	public final void setBatchRead(BatchPolicy policy, Key[] keys, BatchNode batch, String[] binNames, int readAttr) {
		// Estimate full row size
		final int[] offsets = batch.offsets;
		final int max = batch.offsetsSize;
		final int fieldCountRow = policy.sendSetName ? 2 : 1;

		// Calculate size of bin names.
		int binNameSize = 0;
		int operationCount = 0;

		if (binNames != null) {
			for (String binName : binNames) {
				binNameSize += Buffer.estimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
			}
			operationCount = binNames.length;
		}

		// Estimate buffer size.
		begin();
		int fieldCount = 1;
		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}

		dataOffset += FIELD_HEADER_SIZE + 5;

		Key prev = null;

		for (int i = 0; i < max; i++) {
			Key key = keys[offsets[i]];

			dataOffset += key.digest.length + 4;

			// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
			if (prev != null && prev.namespace == key.namespace &&
				(! policy.sendSetName || prev.setName == key.setName)) {
				// Can set repeat previous namespace/bin names to save space.
				dataOffset++;
			}
			else {
				// Must write full header and namespace/set/bin names.
				dataOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE + 6;

				if (policy.sendSetName) {
					dataOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
				}
				dataOffset += binNameSize;
				prev = key;
			}
		}

		sizeBuffer();

		if (policy.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		writeHeaderRead(policy, totalTimeout, readAttr | Command.INFO1_BATCH, fieldCount, 0);

		if (exp != null) {
			dataOffset = exp.write(this);
		}

		int fieldSizeOffset = dataOffset;
		writeFieldHeader(0, policy.sendSetName? FieldType.BATCH_INDEX_WITH_SET : FieldType.BATCH_INDEX);  // Need to update size at end

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
			if (prev != null && prev.namespace == key.namespace &&
				(! policy.sendSetName || prev.setName == key.setName)) {
				// Can set repeat previous namespace/bin names to save space.
				dataBuffer[dataOffset++] = 1;  // repeat
			}
			else {
				// Write full header, namespace and bin names.
				dataBuffer[dataOffset++] = 0;  // do not repeat
				dataBuffer[dataOffset++] = (byte)readAttr;
				Buffer.shortToBytes(fieldCountRow, dataBuffer, dataOffset);
				dataOffset += 2;
				Buffer.shortToBytes(operationCount, dataBuffer, dataOffset);
				dataOffset += 2;
				writeField(key.namespace, FieldType.NAMESPACE);

				if (policy.sendSetName) {
					writeField(key.setName, FieldType.TABLE);
				}

				if (binNames != null) {
					for (String binName : binNames) {
						writeOperation(binName, Operation.Type.READ);
					}
				}
				prev = key;
			}
		}

		// Write real field size.
		Buffer.intToBytes(dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
		end();
		compress(policy);
	}

	public final void setScan(
		ScanPolicy policy,
		String namespace,
		String setName,
		String[] binNames,
		long taskId,
		NodePartitions nodePartitions
	) {
		begin();
		int fieldCount = 0;
		int partsFullSize = 0;
		int partsPartialSize = 0;
		long maxRecords = 0;

		if (nodePartitions != null) {
			partsFullSize = nodePartitions.partsFull.size() * 2;
			partsPartialSize = nodePartitions.partsPartial.size() * 20;
			maxRecords = nodePartitions.recordMax;
		}

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

		CommandExp exp = getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
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
		byte readAttr = Command.INFO1_READ;

		if (! policy.includeBinData) {
			readAttr |= Command.INFO1_NOBINDATA;
		}

		int operationCount = (binNames == null)? 0 : binNames.length;
		writeHeaderRead(policy, totalTimeout, readAttr, fieldCount, operationCount);

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
			writeField(maxRecords, FieldType.SCAN_MAX_RECORDS);
		}

		if (policy.recordsPerSecond > 0) {
			writeField(policy.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
		}

		if (exp != null) {
			dataOffset = exp.write(this);
		}

		// Write scan socket idle timeout.
		writeField(policy.socketTimeout, FieldType.SCAN_TIMEOUT);

		// Write taskId field
		writeField(taskId, FieldType.TRAN_ID);

		if (binNames != null) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		end();
	}

	public final void setQuery(Policy policy, Statement statement, boolean write, NodePartitions nodePartitions) {
		byte[] functionArgBuffer = null;
		int fieldCount = 0;
		int filterSize = 0;
		int binNameSize = 0;
		int partsFullSize = 0;
		int partsPartialSize = 0;
		long maxRecords = 0;

		begin();

		if (statement.getNamespace() != null) {
			dataOffset += Buffer.estimateSizeUtf8(statement.getNamespace()) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (statement.getIndexName() != null) {
			dataOffset += Buffer.estimateSizeUtf8(statement.getIndexName()) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (statement.getSetName() != null) {
			dataOffset += Buffer.estimateSizeUtf8(statement.getSetName()) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Allocate space for TaskId field.
		dataOffset += 8 + FIELD_HEADER_SIZE;
		fieldCount++;

		Filter filter = statement.getFilter();
		String[] binNames = statement.getBinNames();

		if (filter != null) {
			IndexCollectionType type = filter.getCollectionType();

			if (type != IndexCollectionType.DEFAULT) {
				dataOffset += FIELD_HEADER_SIZE + 1;
				fieldCount++;
			}

			dataOffset += FIELD_HEADER_SIZE;
			filterSize++;  // num filters
			filterSize += filter.estimateSize();

			dataOffset += filterSize;
			fieldCount++;

			// Query bin names are specified as a field (Scan bin names are specified later as operations)
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
		else {
			// Calling query with no filters is more efficiently handled by a primary index scan.
			if (nodePartitions != null) {
				partsFullSize = nodePartitions.partsFull.size() * 2;
				partsPartialSize = nodePartitions.partsPartial.size() * 20;
				maxRecords = nodePartitions.recordMax;
			}

			if (partsFullSize > 0) {
				dataOffset += partsFullSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (partsPartialSize > 0) {
				dataOffset += partsPartialSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Estimate max records size;
			if (maxRecords > 0) {
				dataOffset += 8 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Estimate scan timeout size.
			dataOffset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;

			// Estimate records per second size.
			if (statement.getRecordsPerSecond() > 0) {
				dataOffset += 4 + FIELD_HEADER_SIZE;
				fieldCount++;
			}
		}

		PredExp[] predExp = statement.getPredExp();
		CommandExp exp = (predExp != null)? new CommandPredExp(predExp) : getCommandExp(policy);

		if (exp != null) {
			dataOffset += exp.size();
			fieldCount++;
		}

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

		// Operations (used in query execute) and bin names (used in scan/query) are mutually exclusive.
		Operation[] operations = statement.getOperations();
		int operationCount = 0;

		if (operations != null) {
			for (Operation operation : operations) {
				estimateOperationSize(operation);
			}
			operationCount = operations.length;
		}
		else if (binNames != null && filter == null) {
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}
			operationCount = binNames.length;
		}

		sizeBuffer();

		if (write) {
			writeHeaderWrite((WritePolicy)policy, Command.INFO2_WRITE, fieldCount, operationCount);
		}
		else {
			QueryPolicy qp = (QueryPolicy)policy;
			int readAttr = qp.includeBinData ? Command.INFO1_READ : Command.INFO1_READ | Command.INFO1_NOBINDATA;
			writeHeaderRead(policy, totalTimeout, readAttr, fieldCount, operationCount);
		}

		if (statement.getNamespace() != null) {
			writeField(statement.getNamespace(), FieldType.NAMESPACE);
		}

		if (statement.getIndexName() != null) {
			writeField(statement.getIndexName(), FieldType.INDEX_NAME);
		}

		if (statement.getSetName() != null) {
			writeField(statement.getSetName(), FieldType.TABLE);
		}

		// Write taskId field
		writeField(statement.getTaskId(), FieldType.TRAN_ID);

		if (filter != null) {
			IndexCollectionType type = filter.getCollectionType();

			if (type != IndexCollectionType.DEFAULT) {
				writeFieldHeader(1, FieldType.INDEX_TYPE);
				dataBuffer[dataOffset++] = (byte)type.ordinal();
			}

			writeFieldHeader(filterSize, FieldType.INDEX_RANGE);
			dataBuffer[dataOffset++] = (byte)1;
			dataOffset = filter.write(dataBuffer, dataOffset);

			// Query bin names are specified as a field (Scan bin names are specified later as operations)
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
		else {
			// Calling query with no filters is more efficiently handled by a primary index scan.
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
				writeField(maxRecords, FieldType.SCAN_MAX_RECORDS);
			}

			// Write scan socket idle timeout.
			writeField(policy.socketTimeout, FieldType.SCAN_TIMEOUT);

			// Write records per second.
			if (statement.getRecordsPerSecond() > 0) {
				writeField(statement.getRecordsPerSecond(), FieldType.RECORDS_PER_SECOND);
			}
		}

		if (exp != null) {
			dataOffset = exp.write(this);
		}

		if (statement.getFunctionName() != null) {
			writeFieldHeader(1, FieldType.UDF_OP);
			dataBuffer[dataOffset++] = (statement.returnData())? (byte)1 : (byte)2;
			writeField(statement.getPackageName(), FieldType.UDF_PACKAGE_NAME);
			writeField(statement.getFunctionName(), FieldType.UDF_FUNCTION);
			writeField(functionArgBuffer, FieldType.UDF_ARGLIST);
		}

		if (operations != null) {
			for (Operation operation : operations) {
				writeOperation(operation);
			}
		}
		else if (binNames != null && filter == null) {
			// Scan bin names are specified after all fields.
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}

		end();
	}

	private final int estimateKeySize(Policy policy, Key key) {
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

		if (policy.sendKey) {
			dataOffset += key.userKey.estimateSize() + FIELD_HEADER_SIZE + 1;
			fieldCount++;
		}
		return fieldCount;
	}

	private final int estimateUdfSize(String packageName, String functionName, byte[] bytes) {
		dataOffset += Buffer.estimateSizeUtf8(packageName) + FIELD_HEADER_SIZE;
		dataOffset += Buffer.estimateSizeUtf8(functionName) + FIELD_HEADER_SIZE;
		dataOffset += bytes.length + FIELD_HEADER_SIZE;
		return 3;
	}

	private final void estimateOperationSize(Bin bin) throws AerospikeException {
		dataOffset += Buffer.estimateSizeUtf8(bin.name) + OPERATION_HEADER_SIZE;
		dataOffset += bin.value.estimateSize();
	}

	private final void estimateOperationSize(Operation operation) throws AerospikeException {
		dataOffset += Buffer.estimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
		dataOffset += operation.value.estimateSize();
	}

	private final void estimateOperationSize(String binName) {
		dataOffset += Buffer.estimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
	}

	private final void estimateOperationSize() {
		dataOffset += OPERATION_HEADER_SIZE;
	}

	/**
	 * Header write for write commands.
	 */
	private final void writeHeaderWrite(WritePolicy policy, int writeAttr, int fieldCount, int operationCount) {
		// Set flags.
		int generation = 0;
		int readAttr = 0;
		int infoAttr = 0;

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

		if (policy.xdr) {
			readAttr |= Command.INFO1_XDR;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8]  = MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9]  = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		dataBuffer[11] = (byte)infoAttr;
		dataBuffer[12] = 0; // unused
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
	private final void writeHeaderReadWrite(WritePolicy policy, int readAttr, int writeAttr, int fieldCount, int operationCount) {
		// Set flags.
		int generation = 0;
		int infoAttr = 0;

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
		dataBuffer[12] = 0; // unused
		dataBuffer[13] = 0; // clear the result code
		Buffer.intToBytes(generation, dataBuffer, 14);
		Buffer.intToBytes(policy.expiration, dataBuffer, 18);
		Buffer.intToBytes(serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	/**
	 * Header write for read commands.
	 */
	private final void writeHeaderRead(Policy policy, int timeout, int readAttr, int fieldCount, int operationCount) {
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

		if (policy.compress) {
			readAttr |= Command.INFO1_COMPRESS_RESPONSE;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9] = (byte)readAttr;
		dataBuffer[10] = (byte)0;
		dataBuffer[11] = (byte)infoAttr;

		for (int i = 12; i < 22; i++) {
			dataBuffer[i] = 0;
		}
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

		for (int i = 12; i < 22; i++) {
			dataBuffer[i] = 0;
		}
		Buffer.intToBytes(serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	private final void writeKey(Policy policy, Key key) {
		// Write key into buffer.
		if (key.namespace != null) {
			writeField(key.namespace, FieldType.NAMESPACE);
		}

		if (key.setName != null) {
			writeField(key.setName, FieldType.TABLE);
		}

		writeField(key.digest, FieldType.DIGEST_RIPE);

		if (policy.sendKey) {
			writeField(key.userKey, FieldType.KEY);
		}
	}

	private final void writeOperation(Bin bin, Operation.Type operation) throws AerospikeException {
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

	private final void writeOperation(Operation operation) throws AerospikeException {
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

	private final void writeField(Value value, int type) throws AerospikeException {
		int offset = dataOffset + FIELD_HEADER_SIZE;
		dataBuffer[offset++] = (byte)value.getType();
		int len = value.write(dataBuffer, offset) + 1;
		writeFieldHeader(len, type);
		dataOffset += len;
	}

	private final void writeField(String str, int type) {
		int len = Buffer.stringToUtf8(str, dataBuffer, dataOffset + FIELD_HEADER_SIZE);
		writeFieldHeader(len, type);
		dataOffset += len;
	}

	private final void writeField(byte[] bytes, int type) {
		System.arraycopy(bytes, 0, dataBuffer, dataOffset + FIELD_HEADER_SIZE, bytes.length);
		writeFieldHeader(bytes.length, type);
		dataOffset += bytes.length;
	}

	private final void writeField(int val, int type) {
		writeFieldHeader(4, type);
		Buffer.intToBytes(val, dataBuffer, dataOffset);
		dataOffset += 4;
	}

	private final void writeField(long val, int type) {
		writeFieldHeader(8, type);
		Buffer.longToBytes(val, dataBuffer, dataOffset);
		dataOffset += 8;
	}

	private final void writeFieldHeader(int size, int type) {
		Buffer.intToBytes(size+1, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (byte)type;
	}

	public final void writeExpHeader(int size) {
		writeFieldHeader(size, FieldType.FILTER_EXP);
	}

	private final void begin() {
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	private final void end() {
		// Write total size of message which is the current offset.
		long proto = (dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
		Buffer.longToBytes(proto, dataBuffer, 0);
	}

	private final void compress(Policy policy) {
		if (policy.compress && dataOffset > COMPRESS_THRESHOLD) {
			Deflater def = new Deflater();
			try {
				def.setLevel(Deflater.BEST_SPEED);
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

	public static void LogPolicy(Policy p) {
		Log.debug("Policy: " + "socketTimeout=" + p.socketTimeout + " totalTimeout=" + p.totalTimeout + " maxRetries=" + p.maxRetries +
				" sleepBetweenRetries=" + p.sleepBetweenRetries);
	}

	protected final void skipKey(int fieldCount) {
		for (int i = 0; i < fieldCount; i++) {
			int fieldlen = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4 + fieldlen;
		}
	}

	protected final Key parseKey(int fieldCount) {
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
			}
			dataOffset += size;
		}
		return new Key(namespace, digest, setName, userKey);
	}

	protected abstract void sizeBuffer();

	private static final CommandExp getCommandExp(Policy policy) {
		if (policy.filterExp != null) {
			return policy.filterExp;
		}

		if (policy.predExp != null) {
			return new CommandPredExp(policy.predExp);
		}
		return null;
	}

	private static class CommandPredExp implements CommandExp {
		private final PredExp[] predExp;
		private final int sz;

		private CommandPredExp(PredExp[] predExp) {
			this.predExp = predExp;
			this.sz = PredExp.estimateSize(predExp);
		}

		@Override
		public int size() {
			return sz + FIELD_HEADER_SIZE;
		}

		@Override
		public int write(Command cmd) {
			cmd.writeExpHeader(sz);
			return PredExp.write(predExp, cmd.dataBuffer, cmd.dataOffset);
		}
	}
}
