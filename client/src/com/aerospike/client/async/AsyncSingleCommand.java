/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
