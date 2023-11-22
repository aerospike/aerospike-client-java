/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.client.proxy;

import java.util.concurrent.CompletableFuture;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.query.QueryListener;

class RecordSequenceToQueryListener implements RecordSequenceListener {
	private final QueryListener listener;
	private final CompletableFuture<Void> future;

	public RecordSequenceToQueryListener(QueryListener listener, CompletableFuture<Void> future) {
		this.listener = listener;
		this.future = future;
	}

	@Override
	public void onRecord(Key key, Record record) throws AerospikeException {
		listener.onRecord(key, record);
	}

	@Override
	public void onSuccess() {
		future.complete(null);
	}

	@Override
	public void onFailure(AerospikeException ae) {
		future.completeExceptionally(ae);
	}
}