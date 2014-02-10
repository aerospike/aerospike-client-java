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
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;

public final class ScanCommand extends MultiCommand {
	private final ScanPolicy policy;
	private final String namespace;
	private final String setName;
	private final ScanCallback callback;
	private final String[] binNames;

	public ScanCommand(
		Node node,
		ScanPolicy policy,
		String namespace,
		String setName,
		ScanCallback callback,
		String[] binNames
	) {
		super(node);
		this.policy = policy;
		this.namespace = namespace;
		this.setName = setName;
		this.callback = callback;
		this.binNames = binNames;
	}

	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setScan(policy, namespace, setName, binNames);
	}

	protected boolean parseRecordResults(int receiveSize) 
		throws AerospikeException, IOException {
		// Read/parse remaining message bytes one record at a time.
		dataOffset = 0;
		
		while (dataOffset < receiveSize) {
    		readBytes(MSG_REMAINING_HEADER_SIZE);    		
			int resultCode = dataBuffer[5] & 0xFF;

			if (resultCode != 0) {
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
					return false;
				}
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

			// Parse bins.
			Map<String,Object> bins = null;
			
			for (int i = 0 ; i < opCount; i++) {
	    		readBytes(8);	
				int opSize = Buffer.bytesToInt(dataBuffer, 0);
				byte particleType = dataBuffer[5];
				byte nameSize = dataBuffer[7];
	    		
				readBytes(nameSize);
				String name = Buffer.utf8ToString(dataBuffer, 0, nameSize);
		
				int particleBytesSize = (int) (opSize - (4 + nameSize));
				readBytes(particleBytesSize);
		        Object value = Buffer.bytesToParticle(particleType, dataBuffer, 0, particleBytesSize);
						
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
}
