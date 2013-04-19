/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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
	private boolean inHeader = true;
	
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
