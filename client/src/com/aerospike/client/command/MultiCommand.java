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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Node;

public abstract class MultiCommand extends Command {
	private static final int MAX_BUFFER_SIZE = 1024 * 1024;  // 1 MB
	
	private BufferedInputStream bis;
	protected final Node node;
	protected int receiveOffset;
	
	protected MultiCommand(Node node) {
		this.node = node;
		this.sendBuffer = null;
		this.receiveBuffer = null;
	}
	
	protected final Node getNode() { 
		return node;
	}
	
	public final void begin() {
		// Batch, Scan, and Query use buffers allocated on the heap (not thread local).
		sendBuffer = new byte[sendOffset];
		receiveBuffer = new byte[2048];
	}

	protected final void writeHeader(int readAttr, int fieldCount, int operationCount) throws AerospikeException {		
		// Write all header data except total size which must be written last. 
		sendBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		sendBuffer[9] = (byte)readAttr;
		
		for (int i = 10; i < 26; i++) {
			sendBuffer[i] = 0;
		}
		Buffer.shortToBytes(fieldCount, sendBuffer, 26);
		Buffer.shortToBytes(operationCount, sendBuffer, 28);
		sendOffset = MSG_TOTAL_HEADER_SIZE;
	}

	protected final void parseResult(InputStream is) throws AerospikeException, IOException {	
		// Read socket into receive buffer one record at a time.  Do not read entire receive size
		// because the thread local receive buffer would be too big.  Also, scan callbacks can nest 
		// further database commands which contend with the receive buffer.
		bis = new BufferedInputStream(is);
		boolean status = true;
		
    	while (status) {
			// Read header.
    		readBytes(8);

			long size = Buffer.bytesToLong(receiveBuffer, 0);
			int receiveSize = ((int) (size & 0xFFFFFFFFFFFFL));
			
	        if (receiveSize > 0) {
		    	status = parseRecordResults(receiveSize);
			}
	        else {
	        	status = false;
	        }
		}
	}
		
	protected final Key parseKey(int fieldCount) throws IOException {
		byte[] digest = null;
		String namespace = null;
		String setName = null;

		for (int i = 0; i < fieldCount; i++) {
			readBytes(4);	
			int fieldlen = Buffer.bytesToInt(receiveBuffer, 0);
			readBytes(fieldlen);
			int fieldtype = receiveBuffer[0];
			int size = fieldlen - 1;
			
			if (fieldtype == FieldType.DIGEST_RIPE) {
				digest = new byte[size];
				System.arraycopy(receiveBuffer, 1, digest, 0, size);
			}
			else if (fieldtype == FieldType.NAMESPACE) {
				namespace = new String(receiveBuffer, 1, size);
			}				
			else if (fieldtype == FieldType.TABLE) {
				setName = new String(receiveBuffer, 1, size);
			}				
		}
		return new Key(namespace, digest, setName);		
	}

	protected final void readBytes(int length) throws IOException {
		if (length > receiveBuffer.length) {
			// Corrupted data streams can result in a huge length.
			// Do a sanity check here.
			if (length > MAX_BUFFER_SIZE) {
				throw new IllegalArgumentException("Invalid readBytes length: " + length);
			}
			receiveBuffer = new byte[length];
		}
		
		int pos = 0;
		
		while (pos < length) {
			int count = bis.read(receiveBuffer, pos, length - pos);
		    
			if (count < 0) {
		    	throw new EOFException();
			}
			pos += count;
		}		
		receiveOffset += length;
	}
	
	protected abstract boolean parseRecordResults(int receiveSize) throws AerospikeException, IOException;
}
