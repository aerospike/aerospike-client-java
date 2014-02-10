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
package com.aerospike.client.cluster;

import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.command.Buffer;

/**
 * Parse node partitions using old protocol. This is more code than a String.split() implementation, 
 * but it's faster because there are much fewer interim strings.
 */
public final class PartitionTokenizerOld {	
	private static final String ReplicasName = "replicas-write";
	
	// Create reusable StringBuilder for performance.	
	protected final StringBuilder sb;
	protected final byte[] buffer;
	protected int length;
	protected int offset;
	
	public PartitionTokenizerOld(Connection conn) throws AerospikeException {
		// Use low-level info methods and parse byte array directly for maximum performance.
		// Send format:    replicas-write\n
		// Receive format: replicas-write\t<ns1>:<partition id1>;<ns2>:<partition id2>...\n
		Info info = new Info(conn, ReplicasName);
		this.length = info.getLength();

		if (length == 0) {
			throw new AerospikeException.Parse(ReplicasName + " is empty");
		}
		this.buffer = info.getBuffer();
		this.offset = ReplicasName.length() + 1;  // Skip past name and tab
		this.sb = new StringBuilder(32);  // Max namespace length
	}
	
	public HashMap<String,Node[]> updatePartition(HashMap<String,Node[]> map, Node node) throws AerospikeException {		
		Partition partition;
		boolean copied = false;
		
		while ((partition = getNext()) != null) {
			Node[] nodeArray = map.get(partition.namespace);
			
			if (nodeArray == null) {
				if (! copied) {
					// Make shallow copy of map.
					map = new HashMap<String,Node[]>(map);
					copied = true;
				}
				nodeArray = new Node[Node.PARTITIONS];
				map.put(partition.namespace, nodeArray);
			}
			// Log.debug(partition.toString() + ',' + node.getName());
			nodeArray[partition.partitionId] = node;
		}
		return (copied)? map : null;
	}
	
	private Partition getNext() throws AerospikeException {
		int begin = offset;
		
		while (offset < length) {
			if (buffer[offset] == ':') {
				// Parse namespace.
				String namespace = Buffer.utf8ToString(buffer, begin, offset - begin, sb).trim();
				
				if (namespace.length() <= 0 || namespace.length() >= 32) {
					String response = getTruncatedResponse();
					throw new AerospikeException.Parse("Invalid partition namespace " +
						namespace + ". Response=" + response);
				}
				begin = ++offset;
				
				// Parse partition id.
				while (offset < length) {
					byte b = buffer[offset];
					
					if (b == ';' || b == '\n') {
						break;
					}
					offset++;
				}
				
				if (offset == begin) {
					String response = getTruncatedResponse();
					throw new AerospikeException.Parse("Empty partition id for namespace " +
						namespace + ". Response=" + response);										
				}
				
				int partitionId = Buffer.utf8DigitsToInt(buffer, begin, offset);
								
				if (partitionId < 0 || partitionId >= Node.PARTITIONS) {
					String response = getTruncatedResponse();
					String partitionString = Buffer.utf8ToString(buffer, begin, offset - begin);
					throw new AerospikeException.Parse("Invalid partition id " + partitionString +
						" for namespace " + namespace + ". Response=" + response);					
				}
				begin = ++offset;
				return new Partition(namespace, partitionId);
			}
			offset++;
		}
		return null;
	}
	
	private String getTruncatedResponse() {
		int max = (length > 200) ? 200 : length;
		return Buffer.utf8ToString(buffer, 0, max);		
	}	
}
