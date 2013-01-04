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

import java.io.IOException;
import java.io.InputStream;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchExecutor.BatchNamespace;
import com.aerospike.client.policy.Policy;

public abstract class BatchCommand extends Command {
	private final Node node;
	protected int receiveOffset;

	public BatchCommand(Node node) {
		this.node = node;
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
		boolean status = true;
		
		while (status) {
			// Read header.
			readFully(is, receiveBuffer, 8);
	
			long size = Buffer.bytesToLong(receiveBuffer, 0);
			int receiveSize = ((int) (size & 0xFFFFFFFFFFFFL));
			
			// Read remaining message bytes.
	        if (receiveSize > 0) {
	        	resizeReceiveBuffer(receiveSize);
	    		readFully(is, receiveBuffer, receiveSize);
		    	status = parseBatchResults(receiveSize);
			}
	        else {
	        	status = false;
	        }
		}
	}
	
	protected final Key parseKey() {
		int fieldCount = Buffer.bytesToShort(receiveBuffer, receiveOffset + 18);
		receiveOffset += MSG_REMAINING_HEADER_SIZE;

		byte[] digest = null;
		String ns = null;

		for (int i = 0; i < fieldCount; i++) {
			int fieldlen = Buffer.bytesToInt(receiveBuffer, receiveOffset);
			int fieldtype = receiveBuffer[receiveOffset + 4]; 
			
			if (fieldtype == FIELD_TYPE_DIGEST_RIPE) {
				digest = new byte[DIGEST_SIZE];
				System.arraycopy(receiveBuffer, receiveOffset + 5, digest, 0, DIGEST_SIZE);
			}
			else if (fieldtype == FIELD_TYPE_NAMESPACE) {
				//Remember, one byte is used for type out of field
				ns = new String(receiveBuffer, receiveOffset + 5, fieldlen - 1);
			}				
			receiveOffset += 4 + fieldlen;
		}
		return new Key(ns, digest);
	}

	protected abstract void executeBatch(Policy policy, BatchNamespace batchNamespace) throws AerospikeException;
	protected abstract boolean parseBatchResults(int buflen) throws AerospikeException;
}
