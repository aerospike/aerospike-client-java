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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;

public abstract class SingleCommand extends SyncCommand {
	private final Cluster cluster;
	protected final Key key;
	private final Partition partition;

	public SingleCommand(Cluster cluster, Key key) {
		this.cluster = cluster;
		this.key = key;
		this.partition = new Partition(key);
	}
	
	protected final Node getNode() throws AerospikeException.InvalidNode { 
		return cluster.getNode(partition);
	}	

	protected final void emptySocket(Connection conn) throws IOException
	{
		// There should not be any more bytes.
		// Empty the socket to be safe.
		long sz = Buffer.bytesToLong(dataBuffer, 0);
		int headerLength = dataBuffer[8];
		int receiveSize = ((int)(sz & 0xFFFFFFFFFFFFL)) - headerLength;

		// Read remaining message bytes.
		if (receiveSize > 0)
		{
			sizeBuffer(receiveSize);
			conn.readFully(dataBuffer, receiveSize);
		}
	}
}
