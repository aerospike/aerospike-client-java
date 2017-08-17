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
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.ScanPolicy;

public final class AsyncScan extends AsyncMultiCommand {
	private final ScanPolicy policy;
	private final RecordSequenceListener listener;
	private final String namespace;
	private final String setName;
	private final String[] binNames;
	private final long taskId;
	
	public AsyncScan(
		AsyncMultiExecutor parent,
		Node node,
		ScanPolicy policy,
		RecordSequenceListener listener,
		String namespace,
		String setName,
		String[] binNames,
		long taskId
	) {
		super(parent, node, policy, true);
		this.policy = policy;
		this.listener = listener;
		this.namespace = namespace;
		this.setName = setName;
		this.binNames = binNames;
		this.taskId = taskId;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setScan(policy, namespace, setName, binNames, taskId);
	}

	@Override
	protected void parseRow(Key key) throws AerospikeException {		
		Record record = parseRecord();
		listener.onRecord(key, record);
	}
}
