/*
 * Copyright 2012-2015 Aerospike, Inc.
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

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchGetSequenceOld extends AsyncMultiCommand {
	private final BatchNode.BatchNamespace batch;
	private final Policy policy;
	private final Key[] keys;
	private final String[] binNames;
	private final RecordSequenceListener listener;
	private final int readAttr;
	
	public AsyncBatchGetSequenceOld(
		AsyncMultiExecutor parent,
		AsyncCluster cluster,
		AsyncNode node,
		BatchNode.BatchNamespace batch,
		Policy policy,
		Key[] keys,
		String[] binNames,
		RecordSequenceListener listener,
		int readAttr
	) {
		super(parent, cluster, node, false);
		this.batch = batch;
		this.policy = policy;
		this.keys = keys;
		this.binNames = binNames;
		this.listener = listener;
		this.readAttr = readAttr;
	}
		
	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() {
		setBatchReadOld(policy, keys, batch, binNames, readAttr);
	}

	@Override
	protected void parseRow(Key key) {
		if (resultCode == 0) {
			Record record = parseRecord();
			listener.onRecord(key, record);
		}
		else {
			listener.onRecord(key, null);
		}
	}
}
