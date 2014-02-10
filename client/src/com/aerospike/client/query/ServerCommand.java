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
package com.aerospike.client.query;

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;

public final class ServerCommand extends QueryCommand {
	
	public ServerCommand(Node node, Policy policy, Statement statement) {
		super(node, policy, statement);
	}
	
	@Override
	protected boolean parseRecordResults(int receiveSize) 
		throws AerospikeException, IOException {
		// Server commands (Query/Execute UDF) should only send back a return code.
		// Keep parsing logic to empty socket buffer just in case server does
		// send records back.
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
			
			int fieldCount = Buffer.bytesToShort(dataBuffer, 18);
			int opCount = Buffer.bytesToShort(dataBuffer, 20);
			
			parseKey(fieldCount);

			for (int i = 0 ; i < opCount; i++) {
	    		readBytes(8);	
				int opSize = Buffer.bytesToInt(dataBuffer, 0);
				byte nameSize = dataBuffer[7];
	    		
				readBytes(nameSize);
		
				int particleBytesSize = (int) (opSize - (4 + nameSize));
				readBytes(particleBytesSize);
		    }
			
			if (! valid) {
				throw new AerospikeException.QueryTerminated();
			}
		}
		return true;
	}
}
