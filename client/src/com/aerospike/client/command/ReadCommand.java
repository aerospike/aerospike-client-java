/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.policy.Policy;

public class ReadCommand extends SingleCommand {
	private final Policy policy;
	private final String[] binNames;
	private Record record;

	public ReadCommand(Cluster cluster, Policy policy, Key key, String[] binNames) {
		super(cluster, key);
		this.policy = policy;
		this.binNames = binNames;
	}
	
	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setRead(key, binNames);
	}

	protected void parseResult(Connection conn) throws AerospikeException, IOException {
		// Read header.		
		conn.readFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);
	
        // A number of these are commented out because we just don't care enough to read
        // that section of the header. If we do care, uncomment and check!        
		long sz = Buffer.bytesToLong(dataBuffer, 0);
		byte headerLength = dataBuffer[8];
//		byte info1 = header[9];
//		byte info2 = header[10];
//      byte info3 = header[11];
//      byte unused = header[12];
		int resultCode = dataBuffer[13] & 0xFF;
		int generation = Buffer.bytesToInt(dataBuffer, 14);
		int expiration = Buffer.bytesToInt(dataBuffer, 18);
//		int transactionTtl = get_ntohl(header, 22);
		int fieldCount = Buffer.bytesToShort(dataBuffer, 26); // almost certainly 0
		int opCount = Buffer.bytesToShort(dataBuffer, 28);
		int receiveSize = ((int) (sz & 0xFFFFFFFFFFFFL)) - headerLength;
		/*
		byte version = (byte) (((int)(sz >> 56)) & 0xff);
		if (version != MSG_VERSION) {
			if (Log.debugEnabled()) {
				Log.debug("read header: incorrect version.");
			}
		}
		
		byte type = (byte) (((int)(sz >> 48)) & 0xff);
		if (type != MSG_TYPE) {
			if (Log.debugEnabled()) {
				Log.debug("read header: incorrect message type, aborting receive");
			}
		}
		
		if (headerLength != MSG_REMAINING_HEADER_SIZE) {
			if (Log.debugEnabled()) {
				Log.debug("read header: unexpected header size, aborting");
			}
		}*/
				
		// Read remaining message bytes.
        if (receiveSize > 0) {
        	sizeBuffer(receiveSize);
    		conn.readFully(dataBuffer, receiveSize);
        }
        
        if (resultCode != 0) {
        	if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR || resultCode == ResultCode.LARGE_ITEM_NOT_FOUND) {
        		return;
        	}
        	
        	if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				record = parseRecord(opCount, fieldCount, generation, expiration);
				handleUdfError(resultCode);
				return;
        	}
        	throw new AerospikeException(resultCode);
        }
                  
        if (opCount == 0) {
        	// Bin data was not returned.
        	record = new Record(null, generation, expiration);
        	return;
        }
        record = parseRecord(opCount, fieldCount, generation, expiration);            
	}
	
	private void handleUdfError(int resultCode) throws AerospikeException {	
		String ret = (String)record.bins.get("FAILURE");
		
		if (ret == null) {
	    	throw new AerospikeException(resultCode);			
		}
		
		String message;
		int code;
		
		try {
			String[] list = ret.split(":");
			code = Integer.parseInt(list[2].trim());
			
			if (code == ResultCode.LARGE_ITEM_NOT_FOUND) {
				record = null;
				return;
			}
			message = list[0] + ':' + list[1] + ' ' + list[3];
		}
		catch (Exception e) {
			// Use generic exception if parse error occurs.
        	throw new AerospikeException(resultCode, ret);
		}
		
		throw new AerospikeException(code, message);
	}
	
	private final Record parseRecord(
		int opCount, 
		int fieldCount, 
		int generation,
		int expiration
	) throws AerospikeException {
		Map<String,Object> bins = null;
	    int receiveOffset = 0;
	
		// There can be fields in the response (setname etc).
		// But for now, ignore them. Expose them to the API if needed in the future.
		if (fieldCount > 0) {
			// Just skip over all the fields
			for (int i = 0; i < fieldCount; i++) {
				int fieldSize = Buffer.bytesToInt(dataBuffer, receiveOffset);
				receiveOffset += 4 + fieldSize;
			}
		}
	
		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(dataBuffer, receiveOffset);
			byte particleType = dataBuffer[receiveOffset+5];
			byte nameSize = dataBuffer[receiveOffset+7];
			String name = Buffer.utf8ToString(dataBuffer, receiveOffset+8, nameSize);
			receiveOffset += 4 + 4 + nameSize;
	
			int particleBytesSize = (int) (opSize - (4 + nameSize));
	        Object value = Buffer.bytesToParticle(particleType, dataBuffer, receiveOffset, particleBytesSize);
			receiveOffset += particleBytesSize;
	
			if (bins == null) {
				bins = new HashMap<String,Object>();
			}
			bins.put(name, value);
	    }	
	    return new Record(bins, generation, expiration);
	}
	
	public Record getRecord() {
		return record;
	}
}
