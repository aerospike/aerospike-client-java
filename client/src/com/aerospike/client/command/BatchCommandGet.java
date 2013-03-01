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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchExecutor.BatchNamespace;
import com.aerospike.client.policy.Policy;

public final class BatchCommandGet extends MultiCommand {
	private final HashMap<Key,BatchItem> keyMap;
	private final HashSet<String> binNames;
	private final Record[] records;
	private final int readAttr;

	public BatchCommandGet(Node node, HashMap<Key,BatchItem> keyMap, HashSet<String> binNames, Record[] records, int readAttr) {
		super(node);
		this.keyMap = keyMap;
		this.binNames = binNames;
		this.records = records;
		this.readAttr = readAttr;
	}

	public void executeBatch(Policy policy, BatchNamespace batchNamespace) throws AerospikeException {
		// Estimate buffer size
		List<Key> keys = batchNamespace.keys;
		int byteSize = keys.size() * Command.DIGEST_SIZE;

		sendOffset = MSG_TOTAL_HEADER_SIZE + Buffer.estimateSizeUtf8(batchNamespace.namespace) + 
				FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;
		
		if (binNames != null) {
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}			
		}
		
		begin();

		int operationCount = (binNames == null)? 0 : binNames.size();
		writeHeader(readAttr, 2, operationCount);		
		writeField(batchNamespace.namespace, FieldType.NAMESPACE);
		writeFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);
	
		for (Key key : keys) {
			byte[] digest = key.digest;
		    System.arraycopy(digest, 0, sendBuffer, sendOffset, digest.length);
		    sendOffset += digest.length;
		}
		
		if (binNames != null) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}	
		execute(policy);
	}	

	/**
	 * Parse all results in the batch.  Add records to shared list.
	 * If the record was not found, the bins will be null.
	 */
	protected boolean parseRecordResults(int receiveSize) throws AerospikeException, IOException {
		//Parse each message response and add it to the result array
		receiveOffset = 0;
		
		while (receiveOffset < receiveSize) {
    		readBytes(MSG_REMAINING_HEADER_SIZE);    		
			int resultCode = receiveBuffer[5];

			// The only valid server return codes are "ok" and "not found".
			// If other return codes are received, then abort the batch.
			if (resultCode != 0 && resultCode != ResultCode.KEY_NOT_FOUND_ERROR) {
				throw new AerospikeException(resultCode);								
			}
			
			byte info3 = receiveBuffer[3];

			// If this is the end marker of the response, do not proceed further
			if ((info3 & INFO3_LAST) == INFO3_LAST) {
				return false;
			}
			
			int generation = Buffer.bytesToInt(receiveBuffer, 6);
			int expiration = Buffer.bytesToInt(receiveBuffer, 10);
			int fieldCount = Buffer.bytesToShort(receiveBuffer, 18);
			int opCount = Buffer.bytesToShort(receiveBuffer, 20);
			Key key = parseKey(fieldCount);
			BatchItem item = keyMap.get(key);
			
			if (item != null) {				
				if (resultCode == 0) {
					int index = item.getIndex();
					records[index] = parseRecord(opCount, generation, expiration);
				}
			}
			else {
				if (Log.debugEnabled()) {
					Log.debug("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest));
				}
			}
		}
		return true;
	}

	/**
	 * Parses the given byte buffer and populate the result object.
	 * Returns the number of bytes that were parsed from the given buffer.
	 */
	protected Record parseRecord(int opCount, int generation, int expiration) 
		throws AerospikeException, IOException {
		
		Map<String,Object> bins = null;
		ArrayList<Map<String, Object>> duplicates = null;
		
		for (int i = 0 ; i < opCount; i++) {
    		readBytes(8);	
			int opSize = Buffer.bytesToInt(receiveBuffer, 0);
			byte particleType = receiveBuffer[5];
			byte version = receiveBuffer[6];
			byte nameSize = receiveBuffer[7];
			
			readBytes(nameSize);
			String name = Buffer.utf8ToString(receiveBuffer, 0, nameSize);
	
			int particleBytesSize = (int) (opSize - (4 + nameSize));
			readBytes(particleBytesSize);
	        Object value = Buffer.bytesToParticle(particleType, receiveBuffer, 0, particleBytesSize);
	
			// Currently, the batch command returns all the bins even if a subset of
			// the bins are requested. We have to filter it on the client side.
			// TODO: Filter batch bins on server!
			if (binNames == null || binNames.contains(name)) {
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
	    }
	
	    // Remove null duplicates just in case there were holes in the version number space.
	    if (duplicates != null) {
	        while (duplicates.remove(null)) {
	        	;
	        }
	    }
	    return new Record(bins, duplicates, generation, expiration);	    
	}
}
