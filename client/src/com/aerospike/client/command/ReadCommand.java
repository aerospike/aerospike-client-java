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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;

public final class ReadCommand extends SingleCommand {
	private Record record;
	private int resultCode;

	public ReadCommand(Cluster cluster, Key key) {
		super(cluster, key);
	}
	
	protected void parseResult(InputStream is) throws AerospikeException, IOException {
		// Read header.		
		readFully(is, receiveBuffer, MSG_TOTAL_HEADER_SIZE);
	
        // A number of these are commented out because we just don't care enough to read
        // that section of the header. If we do care, uncomment and check!        
		long sz = Buffer.bytesToLong(receiveBuffer, 0);
		byte headerLength = receiveBuffer[8];
//		byte info1 = header[9];
//		byte info2 = header[10];
//      byte info3 = header[11];
//      byte unused = header[12];
		resultCode = receiveBuffer[13] & 0xFF;
		int generation = Buffer.bytesToInt(receiveBuffer, 14);
		int expiration = Buffer.bytesToInt(receiveBuffer, 18);
//		int transactionTtl = get_ntohl(header, 22);
		int fieldCount = Buffer.bytesToShort(receiveBuffer, 26); // almost certainly 0
		int opCount = Buffer.bytesToShort(receiveBuffer, 28);
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
        	resizeReceiveBuffer(receiveSize);
    		readFully(is, receiveBuffer, receiveSize);
        }
        
        if (resultCode != 0) {
        	if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
        		return;
        	}
        	
        	if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
                record = parseRecord(opCount, fieldCount, generation, expiration);
                handleUdfError();
        	}
        	throw new AerospikeException(resultCode);
        }
                  
        if (opCount == 0) {
        	// Bin data was not returned.
        	record = new Record(null, null, generation, expiration);
        	return;
        }
        record = parseRecord(opCount, fieldCount, generation, expiration);            
	}
	
	private void handleUdfError() throws AerospikeException {	
		String ret = (String)record.bins.get("FAILURE");
		
		if (ret != null) {
			String[] list;
			String message;
			int code;
			
			try {
    			list = ret.split(":");
    			code = Integer.parseInt(list[2].trim());
    			message = list[0] + ':' + list[1] + ' ' + list[3];
			}
			catch (Exception e) {
				// Use generic exception if parse error occurs.
	        	throw new AerospikeException(resultCode, ret);
			}
			
			throw new AerospikeException(code, message);
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
	    int receiveOffset = 0;
	
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
	
	public Record getRecord() {
		return record;
	}
	
	public int getResultCode() {
		return resultCode;
	}
}
