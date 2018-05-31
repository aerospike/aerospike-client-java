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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.QueryValidate;

public abstract class MultiCommand extends SyncCommand {
	private static final int MAX_BUFFER_SIZE = 1024 * 1024 * 10;  // 10 MB
	
	private BufferedInputStream bis;
	protected final String namespace;
	private final long clusterKey;
	protected int resultCode;
	protected int generation;
	protected int expiration;
	protected int batchIndex;
	protected int fieldCount;
	protected int opCount;
	private final boolean stopOnNotFound;
	private final boolean first;
	protected volatile boolean valid = true;
	
	protected MultiCommand(boolean stopOnNotFound) {
		this.stopOnNotFound = stopOnNotFound;
		this.namespace = null;
		this.clusterKey = 0;
		this.first = false;
	}
	
	protected MultiCommand(String namespace, long clusterKey, boolean first) {
		this.stopOnNotFound = true;
		this.namespace = namespace;
		this.clusterKey = clusterKey;
		this.first = first;
	}

	public final void execute(Cluster cluster, Policy policy, Node node) {		
		if (clusterKey != 0) {
			if (! first) {
				QueryValidate.validate(node, namespace, clusterKey);
			}
			super.execute(cluster, policy, null, node, true);
			QueryValidate.validate(node, namespace, clusterKey);
		}
		else {			
			super.execute(cluster, policy, null, node, true);
		}
	}

	protected final void parseResult(Connection conn) throws IOException {	
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
		    	status = parseGroup(receiveSize);
			}
		}
	}
		
	/**
	 * Parse all records in the group.
	 */
	private final boolean parseGroup(int receiveSize) throws IOException {
		//Parse each message response and add it to the result array
		dataOffset = 0;
		
		while (dataOffset < receiveSize) {
			readBytes(MSG_REMAINING_HEADER_SIZE);    		
			resultCode = dataBuffer[5] & 0xFF;

			// The only valid server return codes are "ok" and "not found".
			// If other return codes are received, then abort the batch.
			if (resultCode != 0) {
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
					if (stopOnNotFound) {
						return false;
					}
				}
				else {
					throw new AerospikeException(resultCode);
				}
			}
			
			byte info3 = dataBuffer[3];

			// If this is the end marker of the response, do not proceed further
			if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST) {
				return false;
			}
			
			generation = Buffer.bytesToInt(dataBuffer, 6);
			expiration = Buffer.bytesToInt(dataBuffer, 10);
			batchIndex = Buffer.bytesToInt(dataBuffer, 14);
			fieldCount = Buffer.bytesToShort(dataBuffer, 18);
			opCount = Buffer.bytesToShort(dataBuffer, 20);
			
			Key key = parseKey(fieldCount);
			parseRow(key);						
		}
		return true;
	}
	
	protected final Key parseKey(int fieldCount) throws AerospikeException, IOException {
		byte[] digest = null;
		String namespace = null;
		String setName = null;
		Value userKey = null;

		for (int i = 0; i < fieldCount; i++) {
			readBytes(4);	
			int fieldlen = Buffer.bytesToInt(dataBuffer, 0);
			readBytes(fieldlen);
			int fieldtype = dataBuffer[0];
			int size = fieldlen - 1;
			
			switch (fieldtype) {
			case FieldType.DIGEST_RIPE:
				digest = new byte[size];
				System.arraycopy(dataBuffer, 1, digest, 0, size);
				break;
			
			case FieldType.NAMESPACE:
				namespace = Buffer.utf8ToString(dataBuffer, 1, size);
				break;
				
			case FieldType.TABLE:
				setName = Buffer.utf8ToString(dataBuffer, 1, size);
				break;

			case FieldType.KEY:
				userKey = Buffer.bytesToKeyValue(dataBuffer[1], dataBuffer, 2, size-1);
				break;
			}
		}
		return new Key(namespace, digest, setName, userKey);
	}

	/**
	 * Parses the given byte buffer and populate the result object.
	 * Returns the number of bytes that were parsed from the given buffer.
	 */
	protected final Record parseRecord() 
		throws AerospikeException, IOException {
		
		Map<String,Object> bins = null;
		
		for (int i = 0 ; i < opCount; i++) {			
			readBytes(8);	
			int opSize = Buffer.bytesToInt(dataBuffer, 0);
			byte particleType = dataBuffer[5];
			byte nameSize = dataBuffer[7];
			
			readBytes(nameSize);
			String name = Buffer.utf8ToString(dataBuffer, 0, nameSize);
			
			int particleBytesSize = (int) (opSize - (4 + nameSize));
			readBytes(particleBytesSize);
			Object value = Buffer.bytesToParticle(particleType, dataBuffer, 0, particleBytesSize);
			
			if (bins == null) {
				bins = new HashMap<String,Object>();
			}
			bins.put(name, value);
		}
		return new Record(bins, generation, expiration);	    
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

	protected abstract void parseRow(Key key) throws IOException;
}
