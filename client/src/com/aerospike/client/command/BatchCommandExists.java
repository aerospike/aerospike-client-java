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
import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;

public final class BatchCommandExists extends MultiCommand {
	private final BatchNode.BatchNamespace batchNamespace;
	private final Policy policy;
	private final HashMap<Key,BatchItem> keyMap;
	private final boolean[] existsArray;

	public BatchCommandExists(
		Node node,
		BatchNode.BatchNamespace batchNamespace,
		Policy policy,
		HashMap<Key,BatchItem> keyMap,
		boolean[] existsArray
	) {
		super(node);
		this.batchNamespace = batchNamespace;
		this.policy = policy;
		this.keyMap = keyMap;
		this.existsArray = existsArray;
	}
	
	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setBatchExists(batchNamespace);
	}

	/**
	 * Parse all results in the batch.  Add records to shared list.
	 * If the record was not found, the bins will be null.
	 */
	protected boolean parseRecordResults(int receiveSize) throws AerospikeException, IOException {
		//Parse each message response and add it to the result array
		dataOffset = 0;
		
		while (dataOffset < receiveSize) {
			if (! valid) {
				throw new AerospikeException.QueryTerminated();
			}
			
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
			
			int fieldCount = Buffer.bytesToShort(dataBuffer, 18);
			int opCount = Buffer.bytesToShort(dataBuffer, 20);
			
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
