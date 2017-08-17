/*
 * Copyright 2012-2017 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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
package com.aerospike.client.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.QueryPolicy;

public final class QueryRecordExecutor extends QueryExecutor {
	
	private final RecordSet recordSet;
	
	public QueryRecordExecutor(Cluster cluster, QueryPolicy policy, Statement statement, Node node) 
		throws AerospikeException {
		super(cluster, policy, statement, node);
		this.recordSet = new RecordSet(this, policy.recordQueueSize);
		statement.prepare(true);
		initializeThreads();
	}
	
	public void execute() {		
		startThreads();
	}
	
	@Override
	protected MultiCommand createCommand() {
		return new QueryRecordCommand(policy, statement, recordSet);
	}
	
	@Override
	protected void sendCancel() {
		recordSet.abort();
	}

	@Override
	protected void sendCompleted() {		
		recordSet.put(RecordSet.END);
	}
	
	public RecordSet getRecordSet() {
		return recordSet;
	}
}
