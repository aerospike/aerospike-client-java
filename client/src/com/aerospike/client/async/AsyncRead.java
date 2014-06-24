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
package com.aerospike.client.async;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.ThreadLocalData;

public class AsyncRead extends AsyncSingleCommand {
	private final Policy policy;
	private final RecordListener listener;
	private final String[] binNames;
	private Record record;
	
	public AsyncRead(AsyncCluster cluster, Policy policy, RecordListener listener, Key key, String[] binNames) {
		super(cluster, key);
		this.policy = (policy == null) ? new Policy() : policy;
		this.listener = listener;
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

	protected final void parseResult(ByteBuffer byteBuffer) throws AerospikeException {
		dataBuffer = ThreadLocalData.getBuffer();
		
		if (receiveSize > dataBuffer.length) {
			dataBuffer = ThreadLocalData.resizeBuffer(receiveSize);
		}
		// Copy entire message to dataBuffer.
		byteBuffer.position(0);
		byteBuffer.get(dataBuffer, 0, receiveSize);
			
		int resultCode = dataBuffer[5] & 0xFF;
		int generation = Buffer.bytesToInt(dataBuffer, 6);
		int expiration = Buffer.bytesToInt(dataBuffer, 10);
		int fieldCount = Buffer.bytesToShort(dataBuffer, 18);
		int opCount = Buffer.bytesToShort(dataBuffer, 20);
		dataOffset = Command.MSG_REMAINING_HEADER_SIZE;
		        
        if (resultCode == 0) {
            if (opCount == 0) {
            	// Bin data was not returned.
            	record = new Record(null, null, generation, expiration);
            }
            else {
            	record = parseRecord(opCount, fieldCount, generation, expiration);
            }
        }
        else {
        	if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
        		record = null;
        	}
        	else {
        		throw new AerospikeException(resultCode);
        	}
        }
	}
		
	private final Record parseRecord(
		int opCount, 
		int fieldCount, 
		int generation,
		int expiration
	) throws AerospikeException {
		Map<String,Object> bins = null;
		ArrayList<Map<String, Object>> duplicates = null;
	
		// There can be fields in the response (setname etc).
		// But for now, ignore them. Expose them to the API if needed in the future.
		if (fieldCount != 0) {
			// Just skip over all the fields
			for (int i = 0; i < fieldCount; i++) {
				int fieldSize = Buffer.bytesToInt(dataBuffer, dataOffset);
				dataOffset += 4 + fieldSize;
			}
		}
	
		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(dataBuffer, dataOffset);
			byte particleType = dataBuffer[dataOffset+5];
			byte version = dataBuffer[dataOffset+6];
			byte nameSize = dataBuffer[dataOffset+7];
			String name = Buffer.utf8ToString(dataBuffer, dataOffset+8, nameSize);
			dataOffset += 4 + 4 + nameSize;
	
			int particleBytesSize = (int) (opSize - (4 + nameSize));
	        Object value = null;
	        
			value = Buffer.bytesToParticle(particleType, dataBuffer, dataOffset, particleBytesSize);
			dataOffset += particleBytesSize;
	
			Map<String,Object> vmap = null;
			
			if (version > 0 || duplicates != null) {
				if (duplicates == null) {
					duplicates = new ArrayList<Map<String,Object>>(4);
					duplicates.add(bins);
					bins = null;
					
					for (int j = 0; j < version; j++) {
						duplicates.add(null);
					}
				} 
				else {
					for (int j = duplicates.size(); j < version + 1; j++) 
						duplicates.add(null);
				}
	
				vmap = duplicates.get(version);
				if (vmap == null) {
					vmap = new HashMap<String,Object>();
					duplicates.set(version, vmap);
				}
			}
			else {
				if (bins == null) {
					bins = new HashMap<String,Object>();
				}
				vmap = bins;
			}
			vmap.put(name, value);
	    }
	
	    // Remove null duplicates just in case there were holes in the version number space.
	    if (duplicates != null) {
	        while (duplicates.remove(null)) {
	        	;
	        }
	    }
	    return new Record(bins, duplicates, generation, expiration);
	}

	protected final void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key, record);
		}
	}

	protected final void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
