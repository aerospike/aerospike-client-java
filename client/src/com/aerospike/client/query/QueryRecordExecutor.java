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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.QueryPolicy;

public final class QueryRecordExecutor extends QueryExecutor {
	
	private final RecordSet recordSet;
	
	public QueryRecordExecutor(Cluster cluster, QueryPolicy policy, Statement statement) 
		throws AerospikeException {
		super(cluster, policy, statement);
		this.recordSet = new RecordSet(this, policy.recordQueueSize);
	}
	
	public void execute() {		
		startThreads();
	}
	
	@Override
	protected QueryCommand createCommand(Node node) {
		return new QueryRecordCommand(node, policy, statement, recordSet);
	}
	
	@Override
	protected void sendCompleted() {		
		recordSet.put(RecordSet.END);
	}
	
	public RecordSet getRecordSet() {
		return recordSet;
	}
}
