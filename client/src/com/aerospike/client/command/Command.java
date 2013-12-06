/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.command;

import java.util.HashSet;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.MsgPacker;
import com.aerospike.client.util.ThreadLocalData1;

public class Command {		
	// Flags commented out are not supported by this client.
	public static final int INFO1_READ				= (1 << 0); // Contains a read operation.
	public static final int INFO1_GET_ALL			= (1 << 1); // Get all bins.
	//public static final int INFO1_GET_ALL_NODATA 	= (1 << 2); // Get all bins WITHOUT data (currently unimplemented).
	//public static final int INFO1_VERIFY			= (1 << 3); // Verify is a GET transaction that includes data.
	//public static final int INFO1_XDS				= (1 << 4); // Operation is being performed by XDS
	public static final int INFO1_NOBINDATA			= (1 << 5); // Do not read the bins

	public static final int INFO2_WRITE				= (1 << 0); // Create or update record
	public static final int INFO2_DELETE			= (1 << 1); // Fling a record into the belly of Moloch.
	public static final int INFO2_GENERATION		= (1 << 2); // Update if expected generation == old.
	public static final int INFO2_GENERATION_GT		= (1 << 3); // Update if new generation >= old, good for restore.
	public static final int INFO2_GENERATION_DUP	= (1 << 4); // Create a duplicate on a generation collision.
	public static final int INFO2_WRITE_UNIQUE		= (1 << 5); // Fail if record already exists.
	//public static final int INFO2_WRITE_BINUNIQUE	= (1 << 6);
	
	public static final int INFO3_LAST              = (1 << 0); // this is the last of a multi-part message

	public static final int MSG_TOTAL_HEADER_SIZE = 30;
	public static final int FIELD_HEADER_SIZE = 5;
	public static final int OPERATION_HEADER_SIZE = 8;
	public static final int MSG_REMAINING_HEADER_SIZE = 22;
	public static final int DIGEST_SIZE = 20;
	public static final long CL_MSG_VERSION = 2L;
	public static final long AS_MSG_TYPE = 3L;

	protected byte[] sendBuffer;
	protected int sendOffset;

	public Command() {
		this.sendBuffer = ThreadLocalData1.getBuffer();
		this.sendOffset = MSG_TOTAL_HEADER_SIZE;
	}
	
	/**
	 * Sometimes the command is transferred to a different thread.
	 * Must reset sendBuffer when command is used in another thread.
	 */
	public final void resetSendBuffer() {		
		this.sendBuffer = ThreadLocalData1.getBuffer();
	}
	
	public final void setWrite(WritePolicy policy, Operation.Type operation, Key key, Bin[] bins) throws AerospikeException {
		int fieldCount = estimateKeySize(key);
		
		for (Bin bin : bins) {
			estimateOperationSize(bin);
		}		
		begin();
		writeHeader(policy, 0, Command.INFO2_WRITE, fieldCount, bins.length);
		writeKey(key);
				
		for (Bin bin : bins) {
			writeOperation(bin, operation);
		}
		end();
	}

	public void setDelete(WritePolicy policy, Key key) {
		int fieldCount = estimateKeySize(key);
		begin();
		writeHeader(policy, 0, Command.INFO2_WRITE | Command.INFO2_DELETE, fieldCount, 0);
		writeKey(key);
		end();
	}

	public final void setTouch(WritePolicy policy, Key key) {
		int fieldCount = estimateKeySize(key);
		estimateOperationSize();
		begin();
		writeHeader(policy, 0, Command.INFO2_WRITE, fieldCount, 1);
		writeKey(key);
		writeOperation(Operation.Type.TOUCH);
		end();
	}

	public final void setExists(Key key) {
		int fieldCount = estimateKeySize(key);
		begin();
		writeHeader(Command.INFO1_READ | Command.INFO1_NOBINDATA, 0, fieldCount, 0);
		writeKey(key);
		end();
	}

	public final void setRead(Key key) {
		int fieldCount = estimateKeySize(key);
		begin();
		writeHeader(Command.INFO1_READ | Command.INFO1_GET_ALL, 0, fieldCount, 0);
		writeKey(key);
		end();
	}

	public final void setRead(Key key, String[] binNames) {	
		int fieldCount = estimateKeySize(key);
		
		for (String binName : binNames) {
			estimateOperationSize(binName);
		}
		begin();
		writeHeader(Command.INFO1_READ, 0, fieldCount, binNames.length);
		writeKey(key);
		
		for (String binName : binNames) {
			writeOperation(binName, Operation.Type.READ);
		}
		end();
	}

	public final void setReadHeader(Key key) {
		int fieldCount = estimateKeySize(key);
		estimateOperationSize((String)null);
		begin();
		
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
		begin();
		
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
		int fieldCount = estimateKeySize(key);		
		byte[] argBytes = MsgPacker.pack(args);
		fieldCount += estimateUdfSize(packageName, functionName, argBytes);
		
		begin();
		writeHeader(0, Command.INFO2_WRITE, fieldCount, 0);
		writeKey(key);
		writeField(packageName, FieldType.UDF_PACKAGE_NAME);
		writeField(functionName, FieldType.UDF_FUNCTION);
		writeField(argBytes, FieldType.UDF_ARGLIST);
		end();
	}

	public final void setBatchExists(BatchNamespace batchNamespace) {
		// Estimate buffer size
		List<Key> keys = batchNamespace.keys;
		int byteSize = keys.size() * SyncCommand.DIGEST_SIZE;

		sendOffset = MSG_TOTAL_HEADER_SIZE + Buffer.estimateSizeUtf8(batchNamespace.namespace) + 
				FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;
				
		begin();

		writeHeader(Command.INFO1_READ | Command.INFO1_NOBINDATA, 0, 2, 0);
		writeField(batchNamespace.namespace, FieldType.NAMESPACE);
		writeFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);
	
		for (Key key : keys) {
			byte[] digest = key.digest;
		    System.arraycopy(digest, 0, sendBuffer, sendOffset, digest.length);
		    sendOffset += digest.length;
		}
		end();
	}

	public final void setBatchGet(BatchNamespace batchNamespace, HashSet<String> binNames, int readAttr) {
		// Estimate buffer size
		List<Key> keys = batchNamespace.keys;
		int byteSize = keys.size() * SyncCommand.DIGEST_SIZE;

		sendOffset = MSG_TOTAL_HEADER_SIZE + Buffer.estimateSizeUtf8(batchNamespace.namespace) + 
				FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;
		
		if (binNames != null) {
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}			
		}
		
		begin();

		int operationCount = (binNames == null)? 0 : binNames.size();
		writeHeader(readAttr, 0, 2, operationCount);		
		writeField(batchNamespace.namespace, FieldType.NAMESPACE);
		writeFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);
	
		for (Key key : keys) {
			byte[] digest = key.digest;
		    System.arraycopy(digest, 0, sendBuffer, sendOffset, digest.length);
		    sendOffset += digest.length;
		}
		
		if (binNames != null) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		end();
	}
	
	public final void setScan(ScanPolicy policy, String namespace, String setName, String[] binNames) {		
		int fieldCount = 0;
		
		if (namespace != null) {
			sendOffset += Buffer.estimateSizeUtf8(namespace) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (setName != null) {
			sendOffset += Buffer.estimateSizeUtf8(setName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		// Estimate scan options size.
		sendOffset += 2 + FIELD_HEADER_SIZE;
		fieldCount++;

		if (binNames != null) {
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}			
		}

		begin();
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
		sendBuffer[sendOffset++] = priority;
		sendBuffer[sendOffset++] = (byte)policy.scanPercent;
		
		if (binNames != null) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		end();
	}

	public final int estimateKeySize(Key key) {
		int fieldCount = 0;
		
		if (key.namespace != null) {
			sendOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (key.setName != null) {
			sendOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		sendOffset += key.digest.length + FIELD_HEADER_SIZE;
		fieldCount++;
		
		return fieldCount;
	}

	public final int estimateUdfSize(String packageName, String functionName, byte[] bytes) {
		sendOffset += Buffer.estimateSizeUtf8(packageName) + FIELD_HEADER_SIZE;		
		sendOffset += Buffer.estimateSizeUtf8(functionName) + FIELD_HEADER_SIZE;		
		sendOffset += bytes.length;
		return 3;
	}

	public final void estimateOperationSize(Bin bin) throws AerospikeException {
		sendOffset += Buffer.estimateSizeUtf8(bin.name) + OPERATION_HEADER_SIZE;
		sendOffset += bin.value.estimateSize();
	}

	public final void estimateOperationSize(Operation operation) throws AerospikeException {
		sendOffset += Buffer.estimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
		sendOffset += operation.binValue.estimateSize();
	}

	public final void estimateOperationSize(String binName) {
		sendOffset += Buffer.estimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
	}

	public final void estimateOperationSize() {
		sendOffset += OPERATION_HEADER_SIZE;
	}
	
	public final void begin() {
		if (sendOffset > sendBuffer.length) {
			sendBuffer = ThreadLocalData1.resizeBuffer(sendOffset);
		}
	}
	
	/**
	 * Header write for write operations.
	 */
	public final void writeHeader(WritePolicy policy, int readAttr, int writeAttr, int fieldCount, int operationCount) {		   			
        // Set flags.
		int generation = 0;
		int expiration = 0;
    	
    	if (policy != null) {
    		switch (policy.recordExistsAction) {
    		case UPDATE:
    			break;
    		case EXPECT_GEN_EQUAL:
        		generation = policy.generation;    			
        		writeAttr |= Command.INFO2_GENERATION;
    			break;
    		case EXPECT_GEN_GT:
        		generation = policy.generation;    			
        		writeAttr |= Command.INFO2_GENERATION_GT;
    			break;
    		case FAIL:
        		writeAttr |= Command.INFO2_WRITE_UNIQUE;
    			break;
    		case DUPLICATE:
        		generation = policy.generation;    			
        		writeAttr |= Command.INFO2_GENERATION_DUP;
    			break;
    		}
    		expiration = policy.expiration;
    	}
		// Write all header data except total size which must be written last. 
		sendBuffer[8]  = MSG_REMAINING_HEADER_SIZE; // Message header length.
		sendBuffer[9]  = (byte)readAttr;
		sendBuffer[10] = (byte)writeAttr;
		sendBuffer[11] = 0; // info3
		sendBuffer[12] = 0; // unused
		sendBuffer[13] = 0; // clear the result code
		Buffer.intToBytes(generation, sendBuffer, 14);
		Buffer.intToBytes(expiration, sendBuffer, 18);		
		
		// Initialize timeout. It will be written later.
		sendBuffer[22] = 0;
		sendBuffer[23] = 0;
		sendBuffer[24] = 0;
		sendBuffer[25] = 0;
		
		Buffer.shortToBytes(fieldCount, sendBuffer, 26);
		Buffer.shortToBytes(operationCount, sendBuffer, 28);		
		sendOffset = MSG_TOTAL_HEADER_SIZE;
	}

	/**
	 * Generic header write.
	 */
	public final void writeHeader(int readAttr, int writeAttr, int fieldCount, int operationCount) {		
		// Write all header data except total size which must be written last. 
		sendBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		sendBuffer[9] = (byte)readAttr;
		sendBuffer[10] = (byte)writeAttr;
		
		for (int i = 11; i < 26; i++) {
			sendBuffer[i] = 0;
		}
		Buffer.shortToBytes(fieldCount, sendBuffer, 26);
		Buffer.shortToBytes(operationCount, sendBuffer, 28);
		sendOffset = MSG_TOTAL_HEADER_SIZE;
	}

	public final void writeKey(Key key) {
		// Write key into buffer.
		if (key.namespace != null) {
			writeField(key.namespace, FieldType.NAMESPACE);
		}
		
		if (key.setName != null) {
			writeField(key.setName, FieldType.TABLE);
		}
	
		writeField(key.digest, FieldType.DIGEST_RIPE);
	}	

	public final void writeOperation(Bin bin, Operation.Type operation) throws AerospikeException {
        int nameLength = Buffer.stringToUtf8(bin.name, sendBuffer, sendOffset + OPERATION_HEADER_SIZE);
        int valueLength = bin.value.write(sendBuffer, sendOffset + OPERATION_HEADER_SIZE + nameLength);
         
        Buffer.intToBytes(nameLength + valueLength + 4, sendBuffer, sendOffset);
		sendOffset += 4;
        sendBuffer[sendOffset++] = (byte) operation.protocolType;
        sendBuffer[sendOffset++] = (byte) bin.value.getType();
        sendBuffer[sendOffset++] = (byte) 0;
        sendBuffer[sendOffset++] = (byte) nameLength;
        sendOffset += nameLength + valueLength;
	}
		
	public final void writeOperation(Operation operation) throws AerospikeException {
        int nameLength = Buffer.stringToUtf8(operation.binName, sendBuffer, sendOffset + OPERATION_HEADER_SIZE);
        int valueLength = operation.binValue.write(sendBuffer, sendOffset + OPERATION_HEADER_SIZE + nameLength);
         
        Buffer.intToBytes(nameLength + valueLength + 4, sendBuffer, sendOffset);
		sendOffset += 4;
        sendBuffer[sendOffset++] = (byte) operation.type.protocolType;
        sendBuffer[sendOffset++] = (byte) operation.binValue.getType();
        sendBuffer[sendOffset++] = (byte) 0;
        sendBuffer[sendOffset++] = (byte) nameLength;
        sendOffset += nameLength + valueLength;
	}

	public final void writeOperation(String name, Operation.Type operation) {
        int nameLength = Buffer.stringToUtf8(name, sendBuffer, sendOffset + OPERATION_HEADER_SIZE);
         
        Buffer.intToBytes(nameLength + 4, sendBuffer, sendOffset);
		sendOffset += 4;
        sendBuffer[sendOffset++] = (byte) operation.protocolType;
        sendBuffer[sendOffset++] = (byte) 0;
        sendBuffer[sendOffset++] = (byte) 0;
        sendBuffer[sendOffset++] = (byte) nameLength;
        sendOffset += nameLength;
	}

	public final void writeOperation(Operation.Type operation) {
        Buffer.intToBytes(4, sendBuffer, sendOffset);
		sendOffset += 4;
        sendBuffer[sendOffset++] = (byte) operation.protocolType;
        sendBuffer[sendOffset++] = 0;
        sendBuffer[sendOffset++] = 0;
        sendBuffer[sendOffset++] = 0;
	}

	public final void writeField(String str, int type) {
		int len = Buffer.stringToUtf8(str, sendBuffer, sendOffset + FIELD_HEADER_SIZE);
		writeFieldHeader(len, type);
		sendOffset += len;
	}
		
	public final void writeField(byte[] bytes, int type) {
	    System.arraycopy(bytes, 0, sendBuffer, sendOffset + FIELD_HEADER_SIZE, bytes.length);
	    writeFieldHeader(bytes.length, type);
		sendOffset += bytes.length;
	}

	public final void writeFieldHeader(int size, int type) {
		Buffer.intToBytes(size+1, sendBuffer, sendOffset);
		sendOffset += 4;
		sendBuffer[sendOffset++] = (byte)type;
	}

	public final void end() {
		// Write total size of message which is the current offset.
		long size = (sendOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
		Buffer.longToBytes(size, sendBuffer, 0);
	}
	
	public final byte[] getSendBuffer() {
		return sendBuffer;
	}
	
	public final int getSendOffset() {
		return sendOffset;
	}
}
