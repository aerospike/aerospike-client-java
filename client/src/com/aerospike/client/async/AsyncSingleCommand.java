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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Partition;

public abstract class AsyncSingleCommand extends AsyncCommand {
	protected final Key key;
	private final Partition partition;
	protected int receiveSize;
	
	public AsyncSingleCommand(AsyncCluster cluster, Key key) {
		super(cluster);
		this.key = key;
		this.partition = new Partition(key);
	}
	
	protected final AsyncNode getNode() throws AerospikeException.InvalidNode {	
		return (AsyncNode)cluster.getNode(partition);
	}
	
	protected final void read() throws AerospikeException, IOException {
		if (inHeader) {
			if (! conn.read(byteBuffer)) {
				return;
			}
			byteBuffer.position(0);
			receiveSize = ((int) (byteBuffer.getLong() & 0xFFFFFFFFFFFFL));
				        
			if (receiveSize <= byteBuffer.capacity()) {
				byteBuffer.clear();
				byteBuffer.limit(receiveSize);
			}
			else {
				byteBuffer = ByteBuffer.allocateDirect(receiveSize);
			}
			inHeader = false;
		}

		if (! conn.read(byteBuffer)) {
			return;
		}
		parseResult(byteBuffer);
		finish();
	}
			
	protected abstract void parseResult(ByteBuffer byteBuffer) throws AerospikeException;
}
