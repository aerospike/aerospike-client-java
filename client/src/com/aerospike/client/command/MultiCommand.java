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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;

public abstract class MultiCommand extends SyncCommand {
	private static final int MAX_BUFFER_SIZE = 1024 * 1024 * 10;  // 10 MB
	
	private BufferedInputStream bis;
	protected final Node node;
	protected volatile boolean valid = true;
	
	protected MultiCommand(Node node) {
		this.node = node;
	}
	
	protected final Node getNode() { 
		return node;
	}

	protected final void parseResult(Connection conn) throws AerospikeException, IOException {	
		// Read socket into receive buffer one record at a time.  Do not read entire receive size
		// because the thread local receive buffer would be too big.  Also, scan callbacks can nest 
		// further database commands which contend with the receive buffer.
		bis = new BufferedInputStream(conn.getInputStream());
		boolean status = true;
		
    	while (status) {
			// Read header.
    		readBytes(8);

			long size = Buffer.bytesToLong(dataBuffer, 0);
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
			int fieldlen = Buffer.bytesToInt(dataBuffer, 0);
			readBytes(fieldlen);
			int fieldtype = dataBuffer[0];
			int size = fieldlen - 1;
			
			if (fieldtype == FieldType.DIGEST_RIPE) {
				digest = new byte[size];
				System.arraycopy(dataBuffer, 1, digest, 0, size);
			}
			else if (fieldtype == FieldType.NAMESPACE) {
				namespace = Buffer.utf8ToString(dataBuffer, 1, size);
			}				
			else if (fieldtype == FieldType.TABLE) {
				setName = Buffer.utf8ToString(dataBuffer, 1, size);
			}				
		}
		return new Key(namespace, digest, setName);		
	}

	protected final void readBytes(int length) throws IOException {
		if (length > dataBuffer.length) {
			// Corrupted data streams can result in a huge length.
			// Do a sanity check here.
			if (length > MAX_BUFFER_SIZE) {
				throw new IllegalArgumentException("Invalid readBytes length: " + length);
			}
			dataBuffer = new byte[length];
		}
		
		int pos = 0;
		
		while (pos < length) {
			int count = bis.read(dataBuffer, pos, length - pos);
		    
			if (count < 0) {
		    	throw new EOFException();
			}
			pos += count;
		}		
		dataOffset += length;
	}
	
	
	public void stop() {
		valid = false;
	}
	
	public boolean isValid() {
		return valid;
	}

	protected abstract boolean parseRecordResults(int receiveSize) throws AerospikeException, IOException;
}
