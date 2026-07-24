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
package com.aerospike.examples;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionFilter;

public class AsyncScanPage extends AsyncExample {
	private static final String binName = "bin";
	private static final String setName = "apage";
	private static final int size = 50;

	/**
	 * Asynchronous scan example.
	 */
	@Override
	public void runExample() {
		console.info("Write " + size + " records.");
		beginRun();
		WritePhase listener = new WritePhase();
		int launched = 0;

		try {
			for (int i = 1; i <= size; i++) {
				Key key = new Key(namespace(), setName, i);
				Bin bin = new Bin(binName, i);

				launched++;
				client().put(eventLoop(), listener, writePolicy(), key, bin);
			}
		}
		catch (Throwable t) {
			listener.onLaunchFailure(launched, t);
		}
	}

	private void runScan(final int pageSize) throws Exception {

		console.info("Scan max " + pageSize + " records.");

		ScanPolicy policy = new ScanPolicy();
		policy.maxRecords = pageSize;

		PartitionFilter filter = PartitionFilter.all();

		RecordSequenceListener listener = new RecordSequenceListener() {
			private int count = 0;

			@Override
			public void onRecord(Key key, Record record) throws AerospikeException {
				count++;
			}

			@Override
			public void onSuccess() {
				console.info("Records returned: " + count);
				completeRun();
			}

			@Override
			public void onFailure(AerospikeException e) {
				failRun(e);
			}
		};

		client().scanPartitions(eventLoop(), listener, policy, filter, namespace(), setName);
	}

	private class WritePhase implements WriteListener {
		private int launched = size;
		private int completed;
		private Throwable failure;

		public void onSuccess(Key key) {
			onWriteComplete(null);
		}

		public void onFailure(AerospikeException e) {
			onWriteComplete(e);
		}

		private void onLaunchFailure(int launched, Throwable t) {
			this.launched = launched;

			if (failure == null) {
				failure = t;
			}

			if (launched == 0) {
				failRun(failure);
			}
			else if (completed == launched) {
				finish();
			}
		}

		private void onWriteComplete(Throwable t) {
			if (t != null && failure == null) {
				failure = t;
			}

			if (++completed == launched) {
				finish();
			}
		}

		private void finish() {
			if (failure != null) {
				failRun(failure);
				return;
			}

			try {
				beginRun();

				try {
					runScan(30);
					completeRun();
				}
				catch (Throwable t) {
					failRun(t);
					failRun(t);
				}
			}
			catch (Throwable t) {
				failRun(t);
			}
		}
	}
}
