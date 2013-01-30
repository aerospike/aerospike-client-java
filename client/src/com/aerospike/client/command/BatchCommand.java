/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.command;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchExecutor.BatchNamespace;
import com.aerospike.client.policy.Policy;

public abstract class BatchCommand extends Command {
	private final Node node;
	protected final HashMap<Key,Integer> keyMap;
	private BufferedInputStream bis;
	protected int receiveOffset;

	public BatchCommand(Node node, HashMap<Key,Integer> keyMap) {
		this.node = node;
		this.keyMap = keyMap;
	}

	protected final Node getNode() { 
		return node;
	}

	protected final void writeHeader(int readAttr, int operationCount) throws AerospikeException {		
		// Write all header data except total size which must be written last. 
		sendBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		sendBuffer[9] = (byte)readAttr;
		
		for (int i = 10; i < 26; i++) {
			sendBuffer[i] = 0;
		}
		Buffer.shortToBytes(2, sendBuffer, 26);
		Buffer.shortToBytes(operationCount, sendBuffer, 28);
		sendOffset = MSG_TOTAL_HEADER_SIZE;
	}

	protected final void parseResult(InputStream is) throws AerospikeException, IOException {
		// Read socket into receive buffer one record at a time.  Do not read entire receive size
		// because the thread local receive buffer would be too big.
		bis = new BufferedInputStream(is);
		boolean status = true;
		
		while (status) {
			// Read header.
    		readBytes(8);
	
			long size = Buffer.bytesToLong(receiveBuffer, 0);
			int receiveSize = ((int) (size & 0xFFFFFFFFFFFFL));
			
			// Read remaining message bytes.
	        if (receiveSize > 0) {
		    	status = parseBatchResults(receiveSize);
			}
	        else {
	        	status = false;
	        }
		}
	}
	
	protected final Key parseKey() throws IOException {
		int fieldCount = Buffer.bytesToShort(receiveBuffer, 18);

		byte[] digest = null;
		String ns = null;

		for (int i = 0; i < fieldCount; i++) {
    		readBytes(4);	
			int fieldlen = Buffer.bytesToInt(receiveBuffer, 0);
    		readBytes(fieldlen);
			int fieldtype = receiveBuffer[0]; 
			int size = fieldlen - 1;
			
			if (fieldtype == FIELD_TYPE_DIGEST_RIPE) {
				digest = new byte[size];
				System.arraycopy(receiveBuffer, 1, digest, 0, size);
			}
			else if (fieldtype == FIELD_TYPE_NAMESPACE) {
				//Remember, one byte is used for type out of field
				ns = new String(receiveBuffer, 1, size);
			}				
		}
		return new Key(ns, digest);
	}
	
	protected void readBytes(int size) throws IOException {
		resizeReceiveBuffer(size);

		if (bis.read(receiveBuffer, 0, size) != size)
			throw new EOFException();
		
		receiveOffset += size;
	}

	protected abstract void executeBatch(Policy policy, BatchNamespace batchNamespace) throws AerospikeException;
	protected abstract boolean parseBatchResults(int buflen) throws AerospikeException, IOException;
}
