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
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ScanPolicy;

public final class ScanCommand extends Command {
	private final Node node;
	private final ScanCallback callback;

	public ScanCommand(Node node, ScanCallback callback) {
		this.node = node;
		this.callback = callback;
	}

	protected final Node getNode() { 
		return node;
	}
	
	public void scan(ScanPolicy policy, String namespace, String setName) throws AerospikeException {
		int fieldCount = 0;
		
		if (namespace != null) {
			sendOffset += Buffer.estimateSizeUtf8(namespace) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (setName != null) {
			sendOffset += Buffer.estimateSizeUtf8(setName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		// Estimate scan options size.
		sendOffset += 2 + FIELD_HEADER_SIZE;
		fieldCount++;

		begin();
		byte readAttr = Command.INFO1_READ;
		
		if (! policy.includeBinData) {
			readAttr |= Command.INFO1_NOBINDATA;
		}
		
		// Write header data except total size which must be written last. 
		sendBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		sendBuffer[9] = (byte)readAttr;
		
		for (int i = 10; i < 26; i++) {
			sendBuffer[i] = 0;
		}
		Buffer.shortToBytes(fieldCount, sendBuffer, 26);
		sendBuffer[28] = 0;
		sendBuffer[29] = 0;		
		sendOffset = MSG_TOTAL_HEADER_SIZE;
		
		if (namespace != null) {
			writeField(namespace, FIELD_TYPE_NAMESPACE);
		}
		
		if (setName != null) {
			writeField(setName, FIELD_TYPE_TABLE);
		}
	
		writeFieldHeader(2, FIELD_TYPE_SCAN_OPTIONS);
		byte priority = (byte)policy.priority.ordinal();
		priority <<= 4;
		
		if (policy.failOnClusterChange) {
			priority |= 0x08;
		}		
		sendBuffer[sendOffset++] = priority;
		sendBuffer[sendOffset++] = (byte)policy.scanPercent;		
		execute(policy);
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
		    	status = parseScanResults(receiveSize);
			}
	        else {
	        	status = false;
	        }
		}
	}
	
	private boolean parseScanResults(int receiveSize) throws AerospikeException {
		int receiveOffset = 0;
		
		while (receiveOffset < receiveSize) {
			int resultCode = receiveBuffer[receiveOffset + 5];

			if (resultCode != 0) {
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
					return false;
				}
				throw new AerospikeException(resultCode);
			}

			byte info3 = receiveBuffer[receiveOffset + 3];
			
			// If this is the end marker of the response, do not proceed further
			if ((info3 & INFO3_LAST) == INFO3_LAST) {
				return false;
			}
			
			int generation = Buffer.bytesToInt(receiveBuffer, receiveOffset + 6);
			int expiration = Buffer.bytesToInt(receiveBuffer, receiveOffset + 10);
			int fieldCount = Buffer.bytesToShort(receiveBuffer, receiveOffset + 18);
			int opCount = Buffer.bytesToShort(receiveBuffer, receiveOffset + 20);
			
			receiveOffset += MSG_REMAINING_HEADER_SIZE;

			byte[] digest = null;
			String namespace = null;
			String setName = null;

			for (int i = 0; i < fieldCount; i++) {
				int fieldlen = Buffer.bytesToInt(receiveBuffer, receiveOffset);
				int fieldtype = receiveBuffer[receiveOffset + 4]; 
				
				if (fieldtype == FIELD_TYPE_DIGEST_RIPE) {
					digest = new byte[DIGEST_SIZE];
					System.arraycopy(receiveBuffer, receiveOffset + 5, digest, 0, DIGEST_SIZE);
				}
				else if (fieldtype == FIELD_TYPE_NAMESPACE) {
					namespace = new String(receiveBuffer, receiveOffset + 5, fieldlen - 1);
				}				
				else if (fieldtype == FIELD_TYPE_TABLE) {
					setName = new String(receiveBuffer, receiveOffset + 5, fieldlen - 1);
				}				
				receiveOffset += 4 + fieldlen;
			}

			// Parse bins.
			Map<String,Object> bins = null;
			
			for (int i = 0 ; i < opCount; i++) {
				int opSize = Buffer.bytesToInt(receiveBuffer, receiveOffset);
				byte particleType = receiveBuffer[receiveOffset+5];
				byte nameSize = receiveBuffer[receiveOffset+7];
				String name = Buffer.utf8ToString(receiveBuffer, receiveOffset+8, nameSize);
				receiveOffset += 4 + 4 + nameSize;
		
				int particleBytesSize = (int) (opSize - (4 + nameSize));
		        Object value = Buffer.bytesToParticle(particleType, receiveBuffer, receiveOffset, particleBytesSize);
				receiveOffset += particleBytesSize;
						
				if (bins == null) {
					bins = new HashMap<String,Object>();
				}
				bins.put(name, value);
		    }
								
			// Call the callback function.
			callback.scanCallback(namespace, setName, digest, bins, generation, expiration);
		}
		return true;
	}	
}
