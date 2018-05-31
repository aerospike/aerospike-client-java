/*
 * Copyright 2012-2018 Aerospike, Inc.
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

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.ConsistencyLevel;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Packer;

public abstract class Command {		
	// Flags commented out are not supported by this client.
	public static final int INFO1_READ				= (1 << 0); // Contains a read operation.
	public static final int INFO1_GET_ALL			= (1 << 1); // Get all bins.
	public static final int INFO1_BATCH				= (1 << 3); // Batch read or exists.
	public static final int INFO1_NOBINDATA			= (1 << 5); // Do not read the bins.
	public static final int INFO1_CONSISTENCY_ALL	= (1 << 6); // Involve all replicas in read operation.

	public static final int INFO2_WRITE				= (1 << 0); // Create or update record
	public static final int INFO2_DELETE			= (1 << 1); // Fling a record into the belly of Moloch.
	public static final int INFO2_GENERATION		= (1 << 2); // Update if expected generation == old.
	public static final int INFO2_GENERATION_GT		= (1 << 3); // Update if new generation >= old, good for restore.
	public static final int INFO2_DURABLE_DELETE	= (1 << 4); // Transaction resulting in record deletion leaves tombstone (Enterprise only).
	public static final int INFO2_CREATE_ONLY		= (1 << 5); // Create only. Fail if record already exists.
	public static final int INFO2_RESPOND_ALL_OPS	= (1 << 7); // Return a result for every operation.
	
	public static final int INFO3_LAST				= (1 << 0); // This is the last of a multi-part message.
	public static final int INFO3_COMMIT_MASTER		= (1 << 1); // Commit to master only before declaring success.
	public static final int INFO3_UPDATE_ONLY		= (1 << 3); // Update only. Merge bins.
	public static final int INFO3_CREATE_OR_REPLACE	= (1 << 4); // Create or completely replace record.
	public static final int INFO3_REPLACE_ONLY		= (1 << 5); // Completely replace existing record only.
	public static final int INFO3_LINEARIZE_READ	= (1 << 6); // Linearize read when in strong consistency mode.
	
	public static final int MSG_TOTAL_HEADER_SIZE = 30;
	public static final int FIELD_HEADER_SIZE = 5;
	public static final int OPERATION_HEADER_SIZE = 8;
	public static final int MSG_REMAINING_HEADER_SIZE = 22;
	public static final int DIGEST_SIZE = 20;
	public static final long CL_MSG_VERSION = 2L;
	public static final long AS_MSG_TYPE = 3L;

	public byte[] dataBuffer;
	public int dataOffset;
	public int sequence;
	
	public final void setWrite(WritePolicy policy, Operation.Type operation, Key key, Bin[] bins) throws AerospikeException {
		begin();
		int fieldCount = estimateKeySize(policy, key);
				
		for (Bin bin : bins) {
			estimateOperationSize(bin);
		}		
		sizeBuffer();
		writeHeader(policy, 0, Command.INFO2_WRITE, fieldCount, bins.length);
		writeKey(policy, key);
		
		for (Bin bin : bins) {
			writeOperation(bin, operation);
		}
		end();
	}

	public void setDelete(WritePolicy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		sizeBuffer();
		writeHeader(policy, 0, Command.INFO2_WRITE | Command.INFO2_DELETE, fieldCount, 0);
		writeKey(policy, key);
		end();
	}

	public final void setTouch(WritePolicy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		estimateOperationSize();
		sizeBuffer();
		writeHeader(policy, 0, Command.INFO2_WRITE, fieldCount, 1);
		writeKey(policy, key);
		writeOperation(Operation.Type.TOUCH);
		end();
	}

	public final void setExists(Policy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		sizeBuffer();
		writeHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, 0, fieldCount, 0);
		writeKey(policy, key);
		end();
	}

	public final void setRead(Policy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		sizeBuffer();
		writeHeader(policy, Command.INFO1_READ | Command.INFO1_GET_ALL, 0, fieldCount, 0);
		writeKey(policy, key);
		end();
	}

	public final void setRead(Policy policy, Key key, String[] binNames) {
		if (binNames != null) {
			begin();
			int fieldCount = estimateKeySize(policy, key);
			
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}
			sizeBuffer();
			writeHeader(policy, Command.INFO1_READ, 0, fieldCount, binNames.length);
			writeKey(policy, key);
			
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
		estimateOperationSize((String)null);
		sizeBuffer();		
		writeHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, 0, fieldCount, 0);
		writeKey(policy, key);
		end();
	}

	public final void estimateOperate(Operation[] operations, OperateArgs args) {		
		boolean readBin = false;
		boolean readHeader = false;
		boolean respondAllOps = false;
					
		for (Operation operation : operations) {
			switch (operation.type) {
			case MAP_READ:
				// Map operations require respondAllOps to be true.
				respondAllOps = true; 
				// Fall through to read.
			case CDT_READ:
			case READ:
				args.readAttr |= Command.INFO1_READ;
				
				// Read all bins if no bin is specified.
				if (operation.binName == null) {
					args.readAttr |= Command.INFO1_GET_ALL;
				}
				readBin = true;
				break;
				
			case READ_HEADER:
				args.readAttr |= Command.INFO1_READ;
				readHeader = true;
				break;
	
			case MAP_MODIFY:
				// Map operations require respondAllOps to be true.
				respondAllOps = true; 
				// Fall through to write.
			default:
				args.writeAttr = Command.INFO2_WRITE;
				args.hasWrite = true;
				break;		
			}
			estimateOperationSize(operation);
		}
		args.size = dataOffset;
		
		if (readHeader && ! readBin) {
			args.readAttr |= Command.INFO1_NOBINDATA;
		}
		
		if (respondAllOps) {
			args.writeAttr |= Command.INFO2_RESPOND_ALL_OPS;
		}	
	}

	public final void setOperate(WritePolicy policy, Key key, Operation[] operations, OperateArgs args) {
		begin();
		int fieldCount = estimateKeySize(policy, key);
		dataOffset += args.size;					
		sizeBuffer();
				
		writeHeader(policy, args.readAttr, args.writeAttr, fieldCount, operations.length);
		writeKey(policy, key);

		for (Operation operation : operations) {
			writeOperation(operation);
		}
		end();
	}

	public final void setUdf(WritePolicy policy, Key key, String packageName, String functionName, Value[] args) 
		throws AerospikeException {
		begin();
		int fieldCount = estimateKeySize(policy, key);		
		byte[] argBytes = Packer.pack(args);
		fieldCount += estimateUdfSize(packageName, functionName, argBytes);
		
		sizeBuffer();
		writeHeader(policy, 0, Command.INFO2_WRITE, fieldCount, 0);
		writeKey(policy, key);
		writeField(packageName, FieldType.UDF_PACKAGE_NAME);
		writeField(functionName, FieldType.UDF_FUNCTION);
		writeField(argBytes, FieldType.UDF_ARGLIST);
		end();
	}

	public final void setBatchRead(BatchPolicy policy, List<BatchRead> records, BatchNode batch) {
		// Estimate full row size
		final int[] offsets = batch.offsets;
		final int max = batch.offsetsSize;
		final int fieldCount = policy.sendSetName ? 2 : 1;
		BatchRead prev = null;
	    
		begin();
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
		
		if (policy.consistencyLevel == ConsistencyLevel.CONSISTENCY_ALL) {
			readAttr |= Command.INFO1_CONSISTENCY_ALL;
		}

		writeHeader(policy, readAttr | Command.INFO1_BATCH, 0, 1, 0);
		writeHeader(policy, Command.INFO1_READ | Command.INFO1_BATCH, 0, 1, 0);
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
					Buffer.shortToBytes(fieldCount, dataBuffer, dataOffset);
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
					Buffer.shortToBytes(fieldCount, dataBuffer, dataOffset);
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
	}

	public final void setBatchRead(BatchPolicy policy, Key[] keys, BatchNode batch, String[] binNames, int readAttr) {
		// Estimate full row size
		final int[] offsets = batch.offsets;
		final int max = batch.offsetsSize;
		final int fieldCount = policy.sendSetName ? 2 : 1;

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

		if (policy.consistencyLevel == ConsistencyLevel.CONSISTENCY_ALL) {
			readAttr |= Command.INFO1_CONSISTENCY_ALL;
		}

		writeHeader(policy, readAttr | Command.INFO1_BATCH, 0, 1, 0);
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
				Buffer.shortToBytes(fieldCount, dataBuffer, dataOffset);
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
	}

	public final void setBatchReadDirect(Policy policy, Key[] keys, BatchNode.BatchNamespace batch, String[] binNames, int readAttr) {
		// Estimate buffer size
		begin();
		
		int byteSize = batch.offsetsSize * SyncCommand.DIGEST_SIZE;

		dataOffset += Buffer.estimateSizeUtf8(batch.namespace) + 
				FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;
		
		if (binNames != null) {
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}			
		}
		
		sizeBuffer();

		int operationCount = (binNames == null)? 0 : binNames.length;
		writeHeader(policy, readAttr, 0, 2, operationCount);		
		writeField(batch.namespace, FieldType.NAMESPACE);
		writeFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);
	
		int[] offsets = batch.offsets;
		int max = batch.offsetsSize;
		
		for (int i = 0; i < max; i++) {
			Key key = keys[offsets[i]];
			byte[] digest = key.digest;
		    System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
		    dataOffset += digest.length;
		}
		
		if (binNames != null) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		end();
	}

	public final void setScan(ScanPolicy policy, String namespace, String setName, String[] binNames, long taskId) {
		begin();
		int fieldCount = 0;
		
		if (namespace != null) {
			dataOffset += Buffer.estimateSizeUtf8(namespace) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (setName != null) {
			dataOffset += Buffer.estimateSizeUtf8(setName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		// Estimate scan options size.
		dataOffset += 2 + FIELD_HEADER_SIZE;
		fieldCount++;

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
		writeHeader(policy, readAttr, 0, fieldCount, operationCount);
				
		if (namespace != null) {
			writeField(namespace, FieldType.NAMESPACE);
		}
		
		if (setName != null) {
			writeField(setName, FieldType.TABLE);
		}
	
		writeFieldHeader(2, FieldType.SCAN_OPTIONS);
		byte priority = (byte)policy.priority.ordinal();
		priority <<= 4;
		
		if (policy.failOnClusterChange) {
			priority |= 0x08;
		}

		dataBuffer[dataOffset++] = priority;
		dataBuffer[dataOffset++] = (byte)policy.scanPercent;
		
		// Write scan socket idle timeout.
		writeFieldHeader(4, FieldType.SCAN_TIMEOUT);
		Buffer.intToBytes(policy.socketTimeout, dataBuffer, dataOffset);
		dataOffset += 4;

		// Write taskId field
		writeFieldHeader(8, FieldType.TRAN_ID);
		Buffer.longToBytes(taskId, dataBuffer, dataOffset);
		dataOffset += 8;

		if (binNames != null) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		end();
	}

	public final void setQuery(Policy policy, Statement statement, boolean write) {
		byte[] functionArgBuffer = null;
		int fieldCount = 0;
		int filterSize = 0;
		int binNameSize = 0;
		
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
			// Estimate scan options size.
			dataOffset += 2 + FIELD_HEADER_SIZE;
			fieldCount++;

			// Estimate scan timeout size.
			dataOffset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		PredExp[] predExp = statement.getPredExp();
		int predSize = 0;
		
		if (predExp != null) {
			dataOffset += FIELD_HEADER_SIZE;
			predSize = PredExp.estimateSize(predExp);
			dataOffset += predSize;
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

		if (filter == null) {
			if (binNames != null) {
				for (String binName : binNames) {
					estimateOperationSize(binName);
				}
			}
		}

		sizeBuffer();
		
		int operationCount = (filter == null && binNames != null)? binNames.length : 0;
		
		if (write) {
			writeHeader((WritePolicy)policy, Command.INFO1_READ, Command.INFO2_WRITE, fieldCount, operationCount);
		}
		else {
			QueryPolicy qp = (QueryPolicy)policy;
			int readAttr = qp.includeBinData ? Command.INFO1_READ : Command.INFO1_READ | Command.INFO1_NOBINDATA;
			writeHeader(policy, readAttr, 0, fieldCount, operationCount);
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
		writeFieldHeader(8, FieldType.TRAN_ID);
		Buffer.longToBytes(statement.getTaskId(), dataBuffer, dataOffset);
		dataOffset += 8;
		
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
			writeFieldHeader(2, FieldType.SCAN_OPTIONS);
			byte priority = (byte)policy.priority.ordinal();
			priority <<= 4;			
			
			if (! write && ((QueryPolicy)policy).failOnClusterChange) {
				priority |= 0x08;
			}

			dataBuffer[dataOffset++] = priority;
			dataBuffer[dataOffset++] = (byte)100;

			// Write scan socket idle timeout.
			writeFieldHeader(4, FieldType.SCAN_TIMEOUT);
			Buffer.intToBytes(policy.socketTimeout, dataBuffer, dataOffset);
			dataOffset += 4;
		}

		if (predExp != null) {
			writeFieldHeader(predSize, FieldType.PREDEXP);
			dataOffset = PredExp.write(predExp, dataBuffer, dataOffset);
		}

		if (statement.getFunctionName() != null) {
			writeFieldHeader(1, FieldType.UDF_OP);
			dataBuffer[dataOffset++] = (statement.returnData())? (byte)1 : (byte)2;
			writeField(statement.getPackageName(), FieldType.UDF_PACKAGE_NAME);
			writeField(statement.getFunctionName(), FieldType.UDF_FUNCTION);
			writeField(functionArgBuffer, FieldType.UDF_ARGLIST);
		}
		
		// Scan bin names are specified after all fields.
		if (filter == null) {
			if (binNames != null) {
				for (String binName : binNames) {
					writeOperation(binName, Operation.Type.READ);
				}
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

	protected final void estimateOperationSize(String binName) {
		dataOffset += Buffer.estimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
	}

	private final void estimateOperationSize() {
		dataOffset += OPERATION_HEADER_SIZE;
	}
	
	/**
	 * Header write for write operations.
	 */
	protected final void writeHeader(WritePolicy policy, int readAttr, int writeAttr, int fieldCount, int operationCount) {		   			
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
		
		if (policy.linearizeRead) {
			infoAttr |= Command.INFO3_LINEARIZE_READ;
		}

		if (policy.consistencyLevel == ConsistencyLevel.CONSISTENCY_ALL) {
			readAttr |= Command.INFO1_CONSISTENCY_ALL;
		}
		
		if (policy.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
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
		Buffer.intToBytes(policy.totalTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);		
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	/**
	 * Generic header write.
	 */
	protected final void writeHeader(Policy policy, int readAttr, int writeAttr, int fieldCount, int operationCount) {		
		int infoAttr = 0;

		if (policy.linearizeRead) {
			infoAttr |= Command.INFO3_LINEARIZE_READ;
		}

		if (policy.consistencyLevel == ConsistencyLevel.CONSISTENCY_ALL) {
			readAttr |= Command.INFO1_CONSISTENCY_ALL;
		}
		
		// Write all header data except total size which must be written last. 
		dataBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9] = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		dataBuffer[11] = (byte)infoAttr;
		
		for (int i = 12; i < 22; i++) {
			dataBuffer[i] = 0;
		}
		Buffer.intToBytes(policy.totalTimeout, dataBuffer, 22);
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

	protected final void writeOperation(String name, Operation.Type operation) {
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

	public final void writeField(Value value, int type) throws AerospikeException {
		int offset = dataOffset + FIELD_HEADER_SIZE;
		dataBuffer[offset++] = (byte)value.getType();
	    int len = value.write(dataBuffer, offset) + 1;
		writeFieldHeader(len, type);
		dataOffset += len;
	}

	public final void writeField(String str, int type) {
		int len = Buffer.stringToUtf8(str, dataBuffer, dataOffset + FIELD_HEADER_SIZE);
		writeFieldHeader(len, type);
		dataOffset += len;
	}
		
	public final void writeField(byte[] bytes, int type) {
	    System.arraycopy(bytes, 0, dataBuffer, dataOffset + FIELD_HEADER_SIZE, bytes.length);
	    writeFieldHeader(bytes.length, type);
		dataOffset += bytes.length;
	}

	public final void writeFieldHeader(int size, int type) {
		Buffer.intToBytes(size+1, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (byte)type;
	}
	
	protected final void begin() {
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	protected final void end() {
		// Write total size of message which is the current offset.
		long size = (dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
		Buffer.longToBytes(size, dataBuffer, 0);
	}	
	
	public final Node getNode(Cluster cluster, Partition partition, Replica replica, boolean isRead)
	{
		// Handle default case first.
		if (replica == Replica.SEQUENCE) {
			return getSequenceNode(cluster, partition);			
		}
		
		if (replica == Replica.MASTER || ! isRead) {
			return cluster.getMasterNode(partition);
		}

		if (replica == Replica.MASTER_PROLES) {
			return cluster.getMasterProlesNode(partition);			
		}
		return cluster.getRandomNode();
	}

	private final Node getSequenceNode(Cluster cluster, Partition partition)
	{
		// Must copy hashmap reference for copy on write semantics to work.
		HashMap<String,Partitions> map = cluster.partitionMap;
		Partitions partitions = map.get(partition.namespace);
		
		if (partitions == null) {
			throw new AerospikeException("Invalid namespace: " + partition.namespace);
		}

		AtomicReferenceArray<Node>[] replicas = partitions.replicas;
		
		for (int i = 0; i < replicas.length; i++) {
			int index = Math.abs(sequence % replicas.length);						
			Node node = replicas[index].get(partition.partitionId);
			
			if (node != null && node.isActive()) {
				return node;
			}			
			sequence++;
		}

		if (partitions.cpMode) {
			throw new AerospikeException.InvalidNode();
		}
		return cluster.getRandomNode();
	}
	
	protected abstract void sizeBuffer();
}
