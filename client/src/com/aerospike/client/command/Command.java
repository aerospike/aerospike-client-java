/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client.command;

import java.util.HashSet;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Packer;
import com.aerospike.client.util.ThreadLocalData;

public abstract class Command {		
	// Flags commented out are not supported by this client.
	public static final int INFO1_READ				= (1 << 0); // Contains a read operation.
	public static final int INFO1_GET_ALL			= (1 << 1); // Get all bins.
	public static final int INFO1_NOBINDATA			= (1 << 5); // Do not read the bins

	public static final int INFO2_WRITE				= (1 << 0); // Create or update record
	public static final int INFO2_DELETE			= (1 << 1); // Fling a record into the belly of Moloch.
	public static final int INFO2_GENERATION		= (1 << 2); // Update if expected generation == old.
	public static final int INFO2_GENERATION_GT		= (1 << 3); // Update if new generation >= old, good for restore.
	public static final int INFO2_GENERATION_DUP	= (1 << 4); // Create a duplicate on a generation collision.
	public static final int INFO2_CREATE_ONLY		= (1 << 5); // Create only. Fail if record already exists.
	
	public static final int INFO3_LAST              = (1 << 0); // This is the last of a multi-part message.
	public static final int INFO3_UPDATE_ONLY       = (1 << 3); // Update only. Merge bins.
	public static final int INFO3_CREATE_OR_REPLACE = (1 << 4); // Create or completely replace record.
	public static final int INFO3_REPLACE_ONLY      = (1 << 5); // Completely replace existing record only.
	
	public static final int MSG_TOTAL_HEADER_SIZE = 30;
	public static final int FIELD_HEADER_SIZE = 5;
	public static final int OPERATION_HEADER_SIZE = 8;
	public static final int MSG_REMAINING_HEADER_SIZE = 22;
	public static final int DIGEST_SIZE = 20;
	public static final long CL_MSG_VERSION = 2L;
	public static final long AS_MSG_TYPE = 3L;

	protected byte[] dataBuffer;
	protected int dataOffset;
	
	public final void setWrite(WritePolicy policy, Operation.Type operation, Key key, Bin[] bins) throws AerospikeException {
		begin();
		int fieldCount = estimateKeySize(key);
		
		for (Bin bin : bins) {
			estimateOperationSize(bin);
		}		
		sizeBuffer();
		writeHeader(policy, 0, Command.INFO2_WRITE, fieldCount, bins.length);
		writeKey(key);
				
		for (Bin bin : bins) {
			writeOperation(bin, operation);
		}
		end();
	}

	public void setDelete(WritePolicy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(key);
		sizeBuffer();
		writeHeader(policy, 0, Command.INFO2_WRITE | Command.INFO2_DELETE, fieldCount, 0);
		writeKey(key);
		end();
	}

	public final void setTouch(WritePolicy policy, Key key) {
		begin();
		int fieldCount = estimateKeySize(key);
		estimateOperationSize();
		sizeBuffer();
		writeHeader(policy, 0, Command.INFO2_WRITE, fieldCount, 1);
		writeKey(key);
		writeOperation(Operation.Type.TOUCH);
		end();
	}

	public final void setExists(Key key) {
		begin();
		int fieldCount = estimateKeySize(key);
		sizeBuffer();
		writeHeader(Command.INFO1_READ | Command.INFO1_NOBINDATA, 0, fieldCount, 0);
		writeKey(key);
		end();
	}

	public final void setRead(Key key) {
		begin();
		int fieldCount = estimateKeySize(key);
		sizeBuffer();
		writeHeader(Command.INFO1_READ | Command.INFO1_GET_ALL, 0, fieldCount, 0);
		writeKey(key);
		end();
	}

	public final void setRead(Key key, String[] binNames) {
		if (binNames != null) {
			begin();
			int fieldCount = estimateKeySize(key);
			
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}
			sizeBuffer();
			writeHeader(Command.INFO1_READ, 0, fieldCount, binNames.length);
			writeKey(key);
			
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
			end();
		}
		else {
			setRead(key);
		}
	}

	public final void setReadHeader(Key key) {
		begin();
		int fieldCount = estimateKeySize(key);
		estimateOperationSize((String)null);
		sizeBuffer();
		
		// The server does not currently return record header data with INFO1_NOBINDATA attribute set.
		// The workaround is to request a non-existent bin.
		// TODO: Fix this on server.
		//command.setRead(Command.INFO1_READ | Command.INFO1_NOBINDATA);
		writeHeader(Command.INFO1_READ, 0, fieldCount, 1);
		
		writeKey(key);
		writeOperation((String)null, Operation.Type.READ);
		end();
	}

	public final void setOperate(WritePolicy policy, Key key, Operation[] operations) throws AerospikeException {
		begin();
		int fieldCount = estimateKeySize(key);
		int readAttr = 0;
		int writeAttr = 0;
		boolean readHeader = false;
					
		for (Operation operation : operations) {
			switch (operation.type) {
			case READ:
				readAttr |= Command.INFO1_READ;
				
				// Read all bins if no bin is specified.
				if (operation.binName == null) {
					readAttr |= Command.INFO1_GET_ALL;
				}
				break;
				
			case READ_HEADER:
				// The server does not currently return record header data with INFO1_NOBINDATA attribute set.
				// The workaround is to request a non-existent bin.
				// TODO: Fix this on server.
				//readAttr |= Command.INFO1_READ | Command.INFO1_NOBINDATA;
				readAttr |= Command.INFO1_READ;
				readHeader = true;
				break;
				
			default:
				writeAttr = Command.INFO2_WRITE;
				break;				
			}
			estimateOperationSize(operation);
		}
		sizeBuffer();
		
		if (writeAttr != 0) {
			writeHeader(policy, readAttr, writeAttr, fieldCount, operations.length);
		}
		else {
			writeHeader(readAttr, writeAttr, fieldCount, operations.length);			
		}
		writeKey(key);
					
		for (Operation operation : operations) {
			writeOperation(operation);
		}
		
		if (readHeader) {
			writeOperation((String)null, Operation.Type.READ);
		}
		end();
	}

	public final void setUdf(Key key, String packageName, String functionName, Value[] args) 
		throws AerospikeException {
		begin();
		int fieldCount = estimateKeySize(key);		
		byte[] argBytes = Packer.pack(args);
		fieldCount += estimateUdfSize(packageName, functionName, argBytes);
		
		sizeBuffer();
		writeHeader(0, Command.INFO2_WRITE, fieldCount, 0);
		writeKey(key);
		writeField(packageName, FieldType.UDF_PACKAGE_NAME);
		writeField(functionName, FieldType.UDF_FUNCTION);
		writeField(argBytes, FieldType.UDF_ARGLIST);
		end();
	}

	public final void setBatchExists(BatchNamespace batchNamespace) {
		// Estimate buffer size
		begin();
		List<Key> keys = batchNamespace.keys;
		int byteSize = keys.size() * SyncCommand.DIGEST_SIZE;

		dataOffset += Buffer.estimateSizeUtf8(batchNamespace.namespace) + 
				FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;
				
		sizeBuffer();

		writeHeader(Command.INFO1_READ | Command.INFO1_NOBINDATA, 0, 2, 0);
		writeField(batchNamespace.namespace, FieldType.NAMESPACE);
		writeFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);
	
		for (Key key : keys) {
			byte[] digest = key.digest;
		    System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
		    dataOffset += digest.length;
		}
		end();
	}

	public final void setBatchGet(BatchNamespace batchNamespace, HashSet<String> binNames, int readAttr) {
		// Estimate buffer size
		begin();
		List<Key> keys = batchNamespace.keys;
		int byteSize = keys.size() * SyncCommand.DIGEST_SIZE;

		dataOffset += Buffer.estimateSizeUtf8(batchNamespace.namespace) + 
				FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;
		
		if (binNames != null) {
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}			
		}
		
		sizeBuffer();

		int operationCount = (binNames == null)? 0 : binNames.size();
		writeHeader(readAttr, 0, 2, operationCount);		
		writeField(batchNamespace.namespace, FieldType.NAMESPACE);
		writeFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);
	
		for (Key key : keys) {
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
	
	public final void setScan(ScanPolicy policy, String namespace, String setName, String[] binNames) {
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
		writeHeader(readAttr, 0, fieldCount, operationCount);
				
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
		
		if (binNames != null) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		end();
	}

	private final int estimateKeySize(Key key) {
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
		dataOffset += bytes.length;
		return 3;
	}

	private final void estimateOperationSize(Bin bin) throws AerospikeException {
		dataOffset += Buffer.estimateSizeUtf8(bin.name) + OPERATION_HEADER_SIZE;
		dataOffset += bin.value.estimateSize();
	}

	private final void estimateOperationSize(Operation operation) throws AerospikeException {
		dataOffset += Buffer.estimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
		dataOffset += operation.binValue.estimateSize();
	}

	private final void estimateOperationSize(String binName) {
		dataOffset += Buffer.estimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
	}

	private final void estimateOperationSize() {
		dataOffset += OPERATION_HEADER_SIZE;
	}
	
	/**
	 * Header write for write operations.
	 */
	@SuppressWarnings("deprecation")
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
		case FAIL:
    		writeAttr |= Command.INFO2_CREATE_ONLY;
			break;
		// The remaining enums are replaced by "policy.generationPolicy".
		// These enums will eventually be removed.
		// They are handled here for legacy compatibility only.
		case EXPECT_GEN_EQUAL:
    		generation = policy.generation;    			
    		writeAttr |= Command.INFO2_GENERATION;
			break;
		case EXPECT_GEN_GT:
    		generation = policy.generation;    			
    		writeAttr |= Command.INFO2_GENERATION_GT;
			break;
		case DUPLICATE:
    		generation = policy.generation;			
    		writeAttr |= Command.INFO2_GENERATION_DUP;
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
		case DUPLICATE:
    		generation = policy.generation;			
    		writeAttr |= Command.INFO2_GENERATION_DUP;
			break;
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
		
		// Initialize timeout. It will be written later.
		dataBuffer[22] = 0;
		dataBuffer[23] = 0;
		dataBuffer[24] = 0;
		dataBuffer[25] = 0;
		
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);		
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	/**
	 * Generic header write.
	 */
	protected final void writeHeader(int readAttr, int writeAttr, int fieldCount, int operationCount) {		
		// Write all header data except total size which must be written last. 
		dataBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9] = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		
		for (int i = 11; i < 26; i++) {
			dataBuffer[i] = 0;
		}
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = MSG_TOTAL_HEADER_SIZE;
	}

	private final void writeKey(Key key) {
		// Write key into buffer.
		if (key.namespace != null) {
			writeField(key.namespace, FieldType.NAMESPACE);
		}
		
		if (key.setName != null) {
			writeField(key.setName, FieldType.TABLE);
		}
	
		writeField(key.digest, FieldType.DIGEST_RIPE);
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
        int valueLength = operation.binValue.write(dataBuffer, dataOffset + OPERATION_HEADER_SIZE + nameLength);
         
        Buffer.intToBytes(nameLength + valueLength + 4, dataBuffer, dataOffset);
		dataOffset += 4;
        dataBuffer[dataOffset++] = (byte) operation.type.protocolType;
        dataBuffer[dataOffset++] = (byte) operation.binValue.getType();
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

	protected final void sizeBuffer() {
		dataBuffer = ThreadLocalData.getBuffer();
		
		if (dataOffset > dataBuffer.length) {
			dataBuffer = ThreadLocalData.resizeBuffer(dataOffset);
		}
	}
	
	protected final void sizeBuffer(int size) {
		if (size > dataBuffer.length) {
			dataBuffer = ThreadLocalData.resizeBuffer(size);
		}
	}

	protected final void end() {
		// Write total size of message which is the current offset.
		long size = (dataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
		Buffer.longToBytes(size, dataBuffer, 0);
	}
	
	protected abstract Policy getPolicy();
	protected abstract void writeBuffer() throws AerospikeException;
}
