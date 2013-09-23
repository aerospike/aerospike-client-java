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
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Node;

public final class ScanCommand extends MultiCommand {
	private final ScanCallback callback;
	private volatile boolean valid = true;

	public ScanCommand(Node node, ScanCallback callback) {
		super(node);
		this.callback = callback;
	}

	protected boolean parseRecordResults(int receiveSize) 
		throws AerospikeException, IOException {
		// Read/parse remaining message bytes one record at a time.
		receiveOffset = 0;
		
		while (receiveOffset < receiveSize) {
    		readBytes(MSG_REMAINING_HEADER_SIZE);    		
			int resultCode = receiveBuffer[5] & 0xFF;

			if (resultCode != 0) {
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
					return false;
				}
				throw new AerospikeException(resultCode);
			}

			byte info3 = receiveBuffer[3];
			
			// If this is the end marker of the response, do not proceed further
			if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST) {
				return false;
			}
			
			int generation = Buffer.bytesToInt(receiveBuffer, 6);
			int expiration = Buffer.bytesToInt(receiveBuffer, 10);
			int fieldCount = Buffer.bytesToShort(receiveBuffer, 18);
			int opCount = Buffer.bytesToShort(receiveBuffer, 20);
			
			Key key = parseKey(fieldCount);

			// Parse bins.
			Map<String,Object> bins = null;
			
			for (int i = 0 ; i < opCount; i++) {
	    		readBytes(8);	
				int opSize = Buffer.bytesToInt(receiveBuffer, 0);
				byte particleType = receiveBuffer[5];
				byte nameSize = receiveBuffer[7];
	    		
				readBytes(nameSize);
				String name = Buffer.utf8ToString(receiveBuffer, 0, nameSize);
		
				int particleBytesSize = (int) (opSize - (4 + nameSize));
				readBytes(particleBytesSize);
		        Object value = Buffer.bytesToParticle(particleType, receiveBuffer, 0, particleBytesSize);
						
				if (bins == null) {
					bins = new HashMap<String,Object>();
				}
				bins.put(name, value);
		    }
			
			if (! valid) {
				throw new AerospikeException.ScanTerminated();
			}
			
			// Call the callback function.
			callback.scanCallback(key, new Record(bins, null, generation, expiration));
		}
		return true;
	}
	
	public void stop() {
		valid = false;
	}
}
