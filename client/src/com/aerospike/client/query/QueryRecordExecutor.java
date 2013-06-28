/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.query;

import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.QueryPolicy;

public final class QueryRecordExecutor extends QueryExecutor {
	
	private final RecordSet recordSet;
	
	public QueryRecordExecutor(QueryPolicy policy, Statement statement, Node[] nodes) {
		super(policy, statement);
		this.recordSet = new RecordSet(this, policy.recordQueueSize);
		startThreads(nodes);
	}
	
	@Override
	protected QueryCommand createCommand(Node node) {
		return new QueryRecordCommand(node, recordSet);
	}
	
	@Override
	protected void sendCompleted() {		
		recordSet.put(RecordSet.END);
	}
	
	public RecordSet getRecordSet() {
		return recordSet;
	}
}
