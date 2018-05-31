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
package com.aerospike.client.async;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;

public class AsyncRead extends AsyncCommand {
	private final RecordListener listener;
	protected final Key key;
	private final String[] binNames;
	protected Record record;
	
	public AsyncRead(RecordListener listener, Policy policy, Key key, String[] binNames, boolean isRead) {
		super(policy, new Partition(key), null, isRead);
		this.listener = listener;
		this.key = key;
		this.binNames = binNames;
	}

	@Override
	protected void writeBuffer() {
		setRead(policy, key, binNames);
	}

	@Override
	protected final boolean parseResult() {
		validateHeaderSize();
		
		int resultCode = dataBuffer[5] & 0xFF;
		int generation = Buffer.bytesToInt(dataBuffer, 6);
		int expiration = Buffer.bytesToInt(dataBuffer, 10);
		int fieldCount = Buffer.bytesToShort(dataBuffer, 18);
		int opCount = Buffer.bytesToShort(dataBuffer, 20);
		dataOffset = Command.MSG_REMAINING_HEADER_SIZE;
		        
        if (resultCode == 0) {
            if (opCount == 0) {
            	// Bin data was not returned.
            	record = new Record(null, generation, expiration);
            }
            else {
            	record = parseRecord(opCount, fieldCount, generation, expiration);
            }
        }
        else {
        	if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
        		handleNotFound(resultCode);
        	}
        	else {
        		throw new AerospikeException(resultCode);
        	}
        }
        return true;
	}

	protected void handleNotFound(int resultCode) {
		// Do nothing in default case. Record will be null.
	}

	private final Record parseRecord(
		int opCount, 
		int fieldCount, 
		int generation,
		int expiration
	) throws AerospikeException {
		// There can be fields in the response (setname etc).
		// But for now, ignore them. Expose them to the API if needed in the future.
		if (fieldCount > 0) {
			// Just skip over all the fields
			for (int i = 0; i < fieldCount; i++) {
				int fieldSize = Buffer.bytesToInt(dataBuffer, dataOffset);
				dataOffset += 4 + fieldSize;
			}
		}
	
		Map<String,Object> bins = null;
		
		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(dataBuffer, dataOffset);
			byte particleType = dataBuffer[dataOffset+5];
			byte nameSize = dataBuffer[dataOffset+7];
			String name = Buffer.utf8ToString(dataBuffer, dataOffset+8, nameSize);
			dataOffset += 4 + 4 + nameSize;
	
			int particleBytesSize = (int) (opSize - (4 + nameSize));
	        Object value = null;
	        
			value = Buffer.bytesToParticle(particleType, dataBuffer, dataOffset, particleBytesSize);
			dataOffset += particleBytesSize;
	
			if (bins == null) {
				bins = new HashMap<String,Object>();
			}
			addBin(bins, name, value);
	    }	
	    return new Record(bins, generation, expiration);
	}

	protected void addBin(Map<String,Object> bins, String name, Object value) {
		bins.put(name, value);	
	}

	@Override
	protected void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key, record);
		}
	}

	@Override
	protected void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
