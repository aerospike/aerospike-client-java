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

import java.util.Arrays;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;

public final class AsyncBatch {
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------

	public static final class ReadListExecutor extends AsyncMultiExecutor {
		private final BatchListListener listener;
		private final List<BatchRecord> records;

		public ReadListExecutor(
			AsyncCluster cluster,
			BatchPolicy policy, 
			BatchListListener listener,
			List<BatchRecord> records
		) {
			this.listener = listener;
			this.records = records;
			
			// Create commands.
			List<BatchNode> batchNodes = BatchNode.generateList(cluster, policy, records);
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				if (! batchNode.node.hasBatchIndex) {
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Requested command requires a server that supports new batch index protocol.");
				}
				tasks[count++] = new ReadListCommand(this, cluster, batchNode, policy, records);
			}
			// Dispatch commands to nodes.
			execute(tasks, policy.maxConcurrentThreads);
		}
		
		protected void onSuccess() {
			listener.onSuccess(records);
		}
		
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}
	
	private static final class ReadListCommand extends AsyncMultiCommand {
		private final BatchNode batch;
		private final Policy policy;
		private final List<BatchRecord> records;
		
		public ReadListCommand(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			Policy policy,
			List<BatchRecord> records
		) {
			super(parent, cluster, (AsyncNode)batch.node, false);
			this.batch = batch;
			this.policy = policy;
			this.records = records;
		}
			
		@Override
		protected Policy getPolicy() {
			return policy;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, records, batch);
		}

		@Override
		protected void parseRow(Key key) {
			BatchRecord record = records.get(batchIndex);
			
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
	// ReadSequence
	//-------------------------------------------------------
	
	public static final class ReadSequenceExecutor extends AsyncMultiExecutor {
		private final BatchSequenceListener listener;

		public ReadSequenceExecutor(
			AsyncCluster cluster,
			BatchPolicy policy, 
			BatchSequenceListener listener,
			List<BatchRecord> records
		) {
			this.listener = listener;
			
			// Create commands.
			List<BatchNode> batchNodes = BatchNode.generateList(cluster, policy, records);
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				if (! batchNode.node.hasBatchIndex) {
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Requested command requires a server that supports new batch index protocol.");
				}
				tasks[count++] = new ReadSequenceCommand(this, cluster, batchNode, policy, listener, records);
			}
			// Dispatch commands to nodes.
			execute(tasks, policy.maxConcurrentThreads);
		}
		
		protected void onSuccess() {
			listener.onSuccess();
		}
		
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}		
	}
	
	private static final class ReadSequenceCommand extends AsyncMultiCommand {
		private final BatchNode batch;
		private final Policy policy;
		private final BatchSequenceListener listener;
		private final List<BatchRecord> records;
		
		public ReadSequenceCommand(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			Policy policy,
			BatchSequenceListener listener,
			List<BatchRecord> records
		) {
			super(parent, cluster, (AsyncNode)batch.node, false);
			this.batch = batch;
			this.policy = policy;
			this.listener = listener;
			this.records = records;
		}
			
		@Override
		protected Policy getPolicy() {
			return policy;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, records, batch);
		}

		@Override
		protected void parseRow(Key key) {
			BatchRecord record = records.get(batchIndex);
			
			if (Arrays.equals(key.digest, record.key.digest)) {			
				if (resultCode == 0) {
					record.record = parseRecord();
				}
				listener.onRecord(record);
			}
			else {
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.namespace + ',' + Buffer.bytesToHexString(key.digest) + ',' + batchIndex);
			}
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------
	
	public static final class GetArrayExecutor extends BaseExecutor {
		private final RecordArrayListener listener;
		private final Record[] recordArray;

		public GetArrayExecutor(
			AsyncCluster cluster,
			BatchPolicy policy, 
			RecordArrayListener listener,
			Key[] keys,
			String[] binNames,
			int readAttr
		) {
			super(cluster, policy, keys);
			this.recordArray = new Record[keys.length];
			this.listener = listener;
			
			// Create commands.
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[super.taskSize];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {			
				if (batchNode.node.useNewBatch(policy)) {
					// New batch
					tasks[count++] = new GetArrayCommand(this, cluster, batchNode, policy, keys, binNames, recordArray, readAttr);
				}
				else {
					// Old batch only allows one namespace per call.
					for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {				
						tasks[count++] = new GetArrayDirect(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keys, binNames, recordArray, readAttr);
					}
				}
			}
			// Dispatch commands to nodes.
			execute(tasks, policy.maxConcurrentThreads);
		}
		
		protected void onSuccess() {
			listener.onSuccess(keys, recordArray);
		}
		
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}		
	}

	private static final class GetArrayCommand extends AsyncMultiCommand {
		private final BatchNode batch;
		private final Policy policy;
		private final Key[] keys;
		private final String[] binNames;
		private final Record[] records;
		private final int readAttr;
		
		public GetArrayCommand(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			Policy policy,
			Key[] keys,
			String[] binNames,
			Record[] records,
			int readAttr
		) {
			super(parent, cluster, (AsyncNode)batch.node, false);
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}
			
		@Override
		protected Policy getPolicy() {
			return policy;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, keys, batch, binNames, readAttr);
		}

		@Override
		protected void parseRow(Key key) {
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
	
	private static final class GetArrayDirect extends AsyncMultiCommand {
		private final BatchNode.BatchNamespace batch;
		private final Policy policy;
		private final Key[] keys;
		private final String[] binNames;
		private final Record[] records;
		private final int readAttr;
		private int index;
		
		public GetArrayDirect(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			String[] binNames,
			Record[] records,
			int readAttr
		) {
			super(parent, cluster, node, false);
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}
			
		@Override
		protected Policy getPolicy() {
			return policy;
		}

		@Override
		protected void writeBuffer() {
			setBatchReadDirect(policy, keys, batch, binNames, readAttr);
		}

		@Override
		protected void parseRow(Key key) {
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
	// GetSequence
	//-------------------------------------------------------
	
	public static final class GetSequenceExecutor extends BaseExecutor {
		private final RecordSequenceListener listener;

		public GetSequenceExecutor(
			AsyncCluster cluster,
			BatchPolicy policy, 
			RecordSequenceListener listener,
			Key[] keys,
			String[] binNames,
			int readAttr
		) {
			super(cluster, policy, keys);
			this.listener = listener;

			// Create commands.
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[super.taskSize];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {			
				if (batchNode.node.useNewBatch(policy)) {
					// New batch
					tasks[count++] = new GetSequenceCommand(this, cluster, batchNode, policy, keys, binNames, listener, readAttr);
				}
				else {
					// Old batch only allows one namespace per call.
					for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
						tasks[count++] = new GetSequenceDirect(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keys, binNames, listener, readAttr);
					}
				}
			}
			// Dispatch commands to nodes.
			execute(tasks, policy.maxConcurrentThreads);
		}
		
		@Override
		protected void onSuccess() {
			listener.onSuccess();
		}
		
		@Override
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}		
	}
	
	private static final class GetSequenceCommand extends AsyncMultiCommand {
		private final BatchNode batch;
		private final Policy policy;
		private final Key[] keys;
		private final String[] binNames;
		private final RecordSequenceListener listener;
		private final int readAttr;
		
		public GetSequenceCommand(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			Policy policy,
			Key[] keys,
			String[] binNames,
			RecordSequenceListener listener,
			int readAttr
		) {
			super(parent, cluster, (AsyncNode)batch.node, false);
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
			setBatchRead(policy, keys, batch, binNames, readAttr);
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
	
	private static final class GetSequenceDirect extends AsyncMultiCommand {
		private final BatchNode.BatchNamespace batch;
		private final Policy policy;
		private final Key[] keys;
		private final String[] binNames;
		private final RecordSequenceListener listener;
		private final int readAttr;
		
		public GetSequenceDirect(
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
			setBatchReadDirect(policy, keys, batch, binNames, readAttr);
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
	
	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------

	public static final class ExistsArrayExecutor extends BaseExecutor {
		private final ExistsArrayListener listener;
		private final boolean[] existsArray;

		public ExistsArrayExecutor(
			AsyncCluster cluster,
			BatchPolicy policy, 
			Key[] keys,
			ExistsArrayListener listener
		) {
			super(cluster, policy, keys);
			this.existsArray = new boolean[keys.length];
			this.listener = listener;
			
			// Create commands.
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[super.taskSize];
			int count = 0;
			
			for (BatchNode batchNode : batchNodes) {
				if (batchNode.node.useNewBatch(policy)) {
					// New batch
					tasks[count++] = new ExistsArrayCommand(this, cluster, batchNode, policy, keys, existsArray);
				}
				else {
					// Old batch only allows one namespace per call.
					for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
						tasks[count++] = new ExistsArrayDirect(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keys, existsArray);
					}
				}
			}
			// Dispatch commands to nodes.
			execute(tasks, policy.maxConcurrentThreads);
		}
		
		protected void onSuccess() {
			listener.onSuccess(keys, existsArray);
		}
		
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}		
	}
	
	private static final class ExistsArrayCommand extends AsyncMultiCommand {
		private final BatchNode batch;
		private final Policy policy;
		private final Key[] keys;
		private final boolean[] existsArray;
		
		public ExistsArrayCommand(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			Policy policy,
			Key[] keys,
			boolean[] existsArray
		) {
			super(parent, cluster, (AsyncNode)batch.node, false);
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.existsArray = existsArray;
		}
			
		@Override
		protected Policy getPolicy() {
			return policy;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		@Override
		protected void parseRow(Key key) {		
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
	
	private static final class ExistsArrayDirect extends AsyncMultiCommand {
		private final BatchNode.BatchNamespace batch;
		private final Policy policy;
		private final Key[] keys;
		private final boolean[] existsArray;
		private int index;
		
		public ExistsArrayDirect(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			boolean[] existsArray
		) {
			super(parent, cluster, node, false);
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.existsArray = existsArray;
		}
			
		@Override
		protected Policy getPolicy() {
			return policy;
		}

		@Override
		protected void writeBuffer() {
			setBatchReadDirect(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		@Override
		protected void parseRow(Key key) {		
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
	
	//-------------------------------------------------------
	// ExistsSequence
	//-------------------------------------------------------

	public static final class ExistsSequenceExecutor extends BaseExecutor {
		private final ExistsSequenceListener listener;

		public ExistsSequenceExecutor(
			AsyncCluster cluster,
			BatchPolicy policy, 
			Key[] keys,
			ExistsSequenceListener listener
		) {
			super(cluster, policy, keys);
			this.listener = listener;
			
			// Create commands.
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[super.taskSize];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {			
				if (batchNode.node.useNewBatch(policy)) {
					// New batch
					tasks[count++] = new ExistsSequenceCommand(this, cluster, batchNode, policy, keys, listener);
				}
				else {
					// Old batch only allows one namespace per call.
					for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
						tasks[count++] = new ExistsSequenceDirect(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keys, listener);
					}
				}
			}
			// Dispatch commands to nodes.
			execute(tasks, policy.maxConcurrentThreads);
		}
		
		protected void onSuccess() {
			listener.onSuccess();
		}
		
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}		
	}
	
	private static final class ExistsSequenceCommand extends AsyncMultiCommand {
		private final BatchNode batch;
		private final Policy policy;
		private final Key[] keys;
		private final ExistsSequenceListener listener;
		
		public ExistsSequenceCommand(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			Policy policy,
			Key[] keys,
			ExistsSequenceListener listener
		) {
			super(parent, cluster, (AsyncNode)batch.node, false);
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.listener = listener;
		}
			
		@Override
		protected Policy getPolicy() {
			return policy;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		@Override
		protected void parseRow(Key key) {		
			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}			
			listener.onExists(key, resultCode == 0);
		}
	}
	
	private static final class ExistsSequenceDirect extends AsyncMultiCommand {
		private final BatchNode.BatchNamespace batch;
		private final Policy policy;
		private final Key[] keys;
		private final ExistsSequenceListener listener;
		
		public ExistsSequenceDirect(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			ExistsSequenceListener listener
		) {
			super(parent, cluster, node, false);
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.listener = listener;
		}
			
		@Override
		protected Policy getPolicy() {
			return policy;
		}

		@Override
		protected void writeBuffer() {
			setBatchReadDirect(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		@Override
		protected void parseRow(Key key) {		
			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}			
			listener.onExists(key, resultCode == 0);
		}
	}
	
	//-------------------------------------------------------
	// BaseExecutor
	//-------------------------------------------------------

	private static abstract class BaseExecutor extends AsyncMultiExecutor {
		protected final Key[] keys;
		protected final List<BatchNode> batchNodes;
		protected final int taskSize;

		public BaseExecutor(Cluster cluster, BatchPolicy policy, Key[] keys) {
			this.keys = keys;	
			this.batchNodes = BatchNode.generateList(cluster, policy, keys);
			
			// Count number of asynchronous commands needed.
			int size = 0;
			for (BatchNode batchNode : batchNodes) {
				if (batchNode.node.useNewBatch(policy)) {
					// New batch
					size++;
				}
				else {
					// Old batch only allows one namespace per call.
					batchNode.splitByNamespace(keys);
					size += batchNode.batchNamespaces.size();
				}
			}
			this.taskSize = size;
		}	
	}
}
