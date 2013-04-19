/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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
import com.aerospike.client.util.ThreadLocalData1;

public final class AsyncRead extends AsyncSingleCommand {
	private final RecordListener listener;
	private Record record;
	private byte[] receiveBuffer;
	private int receiveOffset;
	
	public AsyncRead(AsyncCluster cluster, Key key, RecordListener listener) {
		super(cluster, key);
		this.listener = listener;
	}

	protected void parseResult(ByteBuffer byteBuffer) throws AerospikeException {
		receiveBuffer = ThreadLocalData1.getBuffer();
		
		if (receiveSize > receiveBuffer.length) {
			receiveBuffer = ThreadLocalData1.resizeBuffer(receiveSize);
		}
		// Copy entire message to receiveBuffer.
		byteBuffer.position(0);
		byteBuffer.get(receiveBuffer, 0, receiveSize);
			
		int resultCode = receiveBuffer[5];
		int generation = Buffer.bytesToInt(receiveBuffer, 6);
		int expiration = Buffer.bytesToInt(receiveBuffer, 10);
		int fieldCount = Buffer.bytesToShort(receiveBuffer, 18);
		int opCount = Buffer.bytesToShort(receiveBuffer, 20);
		receiveOffset = Command.MSG_REMAINING_HEADER_SIZE;
		        
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
				int fieldSize = Buffer.bytesToInt(receiveBuffer, receiveOffset);
				receiveOffset += 4 + fieldSize;
			}
		}
	
		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(receiveBuffer, receiveOffset);
			byte particleType = receiveBuffer[receiveOffset+5];
			byte version = receiveBuffer[receiveOffset+6];
			byte nameSize = receiveBuffer[receiveOffset+7];
			String name = Buffer.utf8ToString(receiveBuffer, receiveOffset+8, nameSize);
			receiveOffset += 4 + 4 + nameSize;
	
			int particleBytesSize = (int) (opSize - (4 + nameSize));
	        Object value = null;
	        
			value = Buffer.bytesToParticle(particleType, receiveBuffer, receiveOffset, particleBytesSize);
			receiveOffset += particleBytesSize;
	
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

	protected void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key, record);
		}
	}

	protected void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
