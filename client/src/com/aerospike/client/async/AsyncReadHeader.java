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
package com.aerospike.client.async;

import java.nio.ByteBuffer;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;

public class AsyncReadHeader extends AsyncSingleCommand {
	private final Policy policy;
	private final RecordListener listener;
	private Record record;
	
	public AsyncReadHeader(AsyncCluster cluster, Policy policy, RecordListener listener, Key key) {
		super(cluster, key);
		this.policy = (policy == null) ? new Policy() : policy;
		this.listener = listener;
	}

	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setReadHeader(key);
	}

	protected final void parseResult(ByteBuffer byteBuffer) throws AerospikeException {
		int resultCode = byteBuffer.get(5) & 0xFF;

        if (resultCode == 0) {
        	int generation = byteBuffer.getInt(6);
    		int expiration = byteBuffer.getInt(10);
    		record = new Record(null, null, generation, expiration);
        }
        else {
        	if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
        		record = null;
        	}
        	else {
        		throw new AerospikeException(resultCode);
        	}
        }		        
	}

	protected final void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key, record);
		}
	}

	protected final void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
