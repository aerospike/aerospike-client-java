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

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.KeyStatus;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchExecutor.BatchNamespace;
import com.aerospike.client.policy.Policy;

public final class BatchCommandExists extends BatchCommand {
	private final List<KeyStatus> keyStatusList;

	public BatchCommandExists(Node node, List<KeyStatus> keyStatusList) {
		super(node);
		this.keyStatusList = keyStatusList;
	}
	
	public void executeBatch(Policy policy, BatchNamespace batchNamespace) throws AerospikeException {
		// Estimate buffer size
		List<Key> keys = batchNamespace.keys;
		int byteSize = keys.size() * BatchCommand.DIGEST_SIZE;

		sendOffset = MSG_TOTAL_HEADER_SIZE + Buffer.estimateSizeUtf8(batchNamespace.namespace) + 
				FIELD_HEADER_SIZE + byteSize + FIELD_HEADER_SIZE;
				
		begin();		
		writeHeader(INFO1_READ | INFO1_NOBINDATA, 0);		
		writeField(batchNamespace.namespace, BatchCommand.FIELD_TYPE_NAMESPACE);
		writeFieldHeader(byteSize, BatchCommand.FIELD_TYPE_DIGEST_RIPE_ARRAY);
	
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
	protected boolean parseBatchResults(int receiveSize) throws AerospikeException {
		//Parse each message response and add it to the result array
		receiveOffset = 0;
		
		while (receiveOffset < receiveSize) {
			int resultCode = receiveBuffer[receiveOffset + 5];

			// The only valid server return codes are "ok" and "not found".
			// If other return codes are received, then abort the batch.
			if (resultCode != 0 && resultCode != ResultCode.KEY_NOT_FOUND_ERROR) {
				throw new AerospikeException(resultCode);								
			}

			byte info3 = receiveBuffer[receiveOffset + 3];
			
			// If this is the end marker of the response, do not proceed further
			if ((info3 & INFO3_LAST) == INFO3_LAST) {
				return false;
			}
			
			int opCount = Buffer.bytesToShort(receiveBuffer, receiveOffset + 20);
			
			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}
						
			Key key = parseKey();
			KeyStatus keyStatus = new KeyStatus(key, resultCode == 0);
			
			synchronized (keyStatusList) {
				keyStatusList.add(keyStatus);
			}
		}
		return true;
	}
}
