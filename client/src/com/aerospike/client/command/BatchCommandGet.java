/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;

public final class BatchCommandGet extends MultiCommand {
	private final BatchNode.BatchNamespace batchNamespace;
	private final Policy policy;
	private final HashMap<Key,BatchItem> keyMap;
	private final HashSet<String> binNames;
	private final Record[] records;
	private final int readAttr;

	public BatchCommandGet(
		Node node,
		BatchNode.BatchNamespace batchNamespace,
		Policy policy,		
		HashMap<Key,BatchItem> keyMap,
		HashSet<String> binNames,
		Record[] records,
		int readAttr
	) {
		super(node);
		this.batchNamespace = batchNamespace;
		this.policy = policy;
		this.keyMap = keyMap;
		this.binNames = binNames;
		this.records = records;
		this.readAttr = readAttr;
	}

	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setBatchGet(batchNamespace, binNames, readAttr);
	}

	/**
	 * Parse all results in the batch.  Add records to shared list.
	 * If the record was not found, the bins will be null.
	 */
	protected boolean parseRecordResults(int receiveSize) throws AerospikeException, IOException {
		//Parse each message response and add it to the result array
		dataOffset = 0;
		
		while (dataOffset < receiveSize) {
    		readBytes(MSG_REMAINING_HEADER_SIZE);    		
			int resultCode = dataBuffer[5] & 0xFF;

			// The only valid server return codes are "ok" and "not found".
			// If other return codes are received, then abort the batch.
			if (resultCode != 0 && resultCode != ResultCode.KEY_NOT_FOUND_ERROR) {
				throw new AerospikeException(resultCode);								
			}
			
			byte info3 = dataBuffer[3];

			// If this is the end marker of the response, do not proceed further
			if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST) {
				return false;
			}
			
			int generation = Buffer.bytesToInt(dataBuffer, 6);
			int expiration = Buffer.bytesToInt(dataBuffer, 10);
			int fieldCount = Buffer.bytesToShort(dataBuffer, 18);
			int opCount = Buffer.bytesToShort(dataBuffer, 20);
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
			if (! valid) {
				throw new AerospikeException.QueryTerminated();
			}
			
    		readBytes(8);	
			int opSize = Buffer.bytesToInt(dataBuffer, 0);
			byte particleType = dataBuffer[5];
			byte version = dataBuffer[6];
			byte nameSize = dataBuffer[7];
			
			readBytes(nameSize);
			String name = Buffer.utf8ToString(dataBuffer, 0, nameSize);
	
			int particleBytesSize = (int) (opSize - (4 + nameSize));
			readBytes(particleBytesSize);
	        Object value = Buffer.bytesToParticle(particleType, dataBuffer, 0, particleBytesSize);
	
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
