/*
 * Copyright 2012-2018 Aerospike, Inc.
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
package com.aerospike.client.command;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;

public final class Batch {
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------
	
	public static final class ReadListCommand extends MultiCommand {
		private final BatchNode batch;
		private final BatchPolicy policy;
		private final List<BatchRead> records;

		public ReadListCommand(BatchNode batch, BatchPolicy policy, List<BatchRead> records) {
			super(false);
			this.batch = batch;
			this.policy = policy;
			this.records = records;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, records, batch);
		}

		@Override
		protected void parseRow(Key key) throws IOException {
			BatchRead record = records.get(batchIndex);
			
			if (Arrays.equals(key.digest, record.key.digest)) {
				if (resultCode == 0) {
					record.record = parseRecord();
				}
			}
			else {
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest) + ',' + batchIndex);
			}
		}
	}
	
	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------
	
	public static final class GetArrayCommand extends MultiCommand {
		private final BatchNode batch;
		private final BatchPolicy policy;
		private final Key[] keys;
		private final String[] binNames;
		private final Record[] records;
		private final int readAttr;

		public GetArrayCommand(
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			String[] binNames,
			Record[] records,
			int readAttr
		) {
			super(false);
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, keys, batch, binNames, readAttr);
		}

		@Override
		protected void parseRow(Key key) throws IOException {
			if (Arrays.equals(key.digest, keys[batchIndex].digest)) {			
				if (resultCode == 0) {
					records[batchIndex] = parseRecord();
				}
			}
			else {
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest) + ',' + batchIndex);
			}
		}
	}
	
	public static final class GetArrayDirect extends MultiCommand {
		private final BatchNode.BatchNamespace batch;
		private final Policy policy;
		private final Key[] keys;
		private final String[] binNames;
		private final Record[] records;
		private final int readAttr;
		private int index;

		public GetArrayDirect(
			BatchNode.BatchNamespace batch,
			Policy policy,		
			Key[] keys,
			String[] binNames,
			Record[] records,
			int readAttr
		) {
			super(false);
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		@Override
		protected void writeBuffer() {
			setBatchReadDirect(policy, keys, batch, binNames, readAttr);
		}

		@Override
		protected void parseRow(Key key) throws IOException {
			int offset = batch.offsets[index++];
			
			if (Arrays.equals(key.digest, keys[offset].digest)) {			
				if (resultCode == 0) {
					records[offset] = parseRecord();
				}
			}
			else {
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest) + ',' + index + ',' + offset);
			}
		}
	}
	
	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------

	public static final class ExistsArrayCommand extends MultiCommand {
		private final BatchNode batch;
		private final BatchPolicy policy;
		private final Key[] keys;
		private final boolean[] existsArray;

		public ExistsArrayCommand(
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			boolean[] existsArray
		) {
			super(false);
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.existsArray = existsArray;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		@Override
		protected void parseRow(Key key) throws IOException {
			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}
			
			if (Arrays.equals(key.digest, keys[batchIndex].digest)) {
				existsArray[batchIndex] = resultCode == 0;
			}
			else {
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest) + ',' + batchIndex);
			}
		}
	}
	
	public static final class ExistsArrayDirect extends MultiCommand {
		private final BatchNode.BatchNamespace batch;
		private final Policy policy;
		private final Key[] keys;
		private final boolean[] existsArray;
		private int index;

		public ExistsArrayDirect(
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			boolean[] existsArray
		) {
			super(false);
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.existsArray = existsArray;
		}
		
		@Override
		protected void writeBuffer() {
			setBatchReadDirect(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		@Override
		protected void parseRow(Key key) throws IOException {
			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}
			
			int offset = batch.offsets[index++];
			
			if (Arrays.equals(key.digest, keys[offset].digest)) {
				existsArray[offset] = resultCode == 0;
			}
			else {
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest) + ',' + index + ',' + offset);
			}
		}
	}	
}
