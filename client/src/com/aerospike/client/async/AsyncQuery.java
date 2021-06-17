/*
 * Copyright 2012-2021 Aerospike, Inc.
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
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;

public final class AsyncQuery extends AsyncMultiCommand {
	private final RecordSequenceListener listener;
	private final Statement statement;

	public AsyncQuery(
		AsyncMultiExecutor parent,
		Node node,
		QueryPolicy policy,
		RecordSequenceListener listener,
		Statement statement
	) {
		super(parent, node, policy, policy.socketTimeout, policy.totalTimeout);
		this.listener = listener;
		this.statement = statement;
		deserializeKeys = true;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setQuery(policy, statement, false, null);
	}

	@Override
	protected void parseRow(Key key) throws AerospikeException {
		Record record = parseRecord();
		listener.onRecord(key, record);
	}
}
