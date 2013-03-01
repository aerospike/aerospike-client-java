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
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchExecutor.BatchNamespace;
import com.aerospike.client.policy.Policy;

public final class BatchCommandExists extends MultiCommand {
	private final HashMap<Key,BatchItem> keyMap;
	private final boolean[] existsArray;

	public BatchCommandExists(Node node, HashMap<Key,BatchItem> keyMap, boolean[] existsArray) {
		super(node);
		this.keyMap = keyMap;
		this.existsArray = existsArray;
	}
	
	public void executeBatch(Policy policy, BatchNamespace batchNamespace) throws AerospikeException {
		// Estimate buffer size
		List<Key> keys = batchNamespace.keys;
		int byteSize = keys.size() * Command.DIGEST_SIZE;

		sendOffset = MSG_TOTAL_HEADER_SIZE + Buffer.estimateSizeUtf8(batchNamespace.namespace) + 
				FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;
				
		begin();

		writeHeader(INFO1_READ | INFO1_NOBINDATA, 2, 0);		
		writeField(batchNamespace.namespace, FieldType.NAMESPACE);
		writeFieldHeader(byteSize, FieldType.DIGEST_RIPE_ARRAY);
	
		for (Key key : keys) {
			byte[] digest = key.digest;
		    System.arraycopy(digest, 0, sendBuffer, sendOffset, digest.length);
		    sendOffset += digest.length;
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
			
			int fieldCount = Buffer.bytesToShort(receiveBuffer, 18);
			int opCount = Buffer.bytesToShort(receiveBuffer, 20);
			
			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}
						
			Key key = parseKey(fieldCount);
			BatchItem item = keyMap.get(key);
			
			if (item != null) {
				int index = item.getIndex();
				existsArray[index] = resultCode == 0;
			}
			else {
				if (Log.debugEnabled()) {
					Log.debug("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest));
				}
			}
		}
		return true;
	}
}
