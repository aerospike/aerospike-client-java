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
package com.aerospike.client.task;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;

/**
 * Task used to poll for long running create index completion.
 */
public final class IndexTask extends Task {
	private final String namespace;
	private final String indexName;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public IndexTask(Cluster cluster, String namespace, String indexName) {
		super(cluster, false);
		this.namespace = namespace;
		this.indexName = indexName;
	}

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public IndexTask() {
		super(null, true);
		namespace = null;
		indexName = null;
	}

	/**
	 * Query all nodes for task completion status.
	 */
	@Override
	public boolean isDone() throws AerospikeException {
		String command = "sindex/" + namespace + '/' + indexName;
		Node[] nodes = cluster.getNodes();
		boolean complete = false;

		for (Node node : nodes) {
			try {
				String response = Info.request(node, command);
				String find = "load_pct=";
				int index = response.indexOf(find);
	
				if (index < 0) {
					complete = true;
					continue;
				}
	
				int begin = index + find.length();
				int end = response.indexOf(';', begin);
				String str = response.substring(begin, end);
				int pct = Integer.parseInt(str);
				
				if (pct >= 0 && pct < 100) {
					return false;
				}
				complete = true;
			}
			catch (Exception e) {
				complete = true;
			}
		}
		return complete;
	}
}
