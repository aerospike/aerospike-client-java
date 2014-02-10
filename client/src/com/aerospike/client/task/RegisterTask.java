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
 * Task used to poll for UDF registration completion.
 */
public final class RegisterTask extends Task {
	private final String packageName;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public RegisterTask(Cluster cluster, String packageName) {
		super(cluster, false);
		this.packageName = packageName;
	}

	/**
	 * Query all nodes for task completion status.
	 */
	public boolean isDone() throws AerospikeException {
		String command = "udf-list";
		Node[] nodes = cluster.getNodes();
		boolean done = false;

		for (Node node : nodes) {
			String response = Info.request(node, command);
			String find = "filename=" + packageName;
			int index = response.indexOf(find);

			if (index < 0) {
				return false;
			}
			done = true;
		}
		return done;
	}
}
