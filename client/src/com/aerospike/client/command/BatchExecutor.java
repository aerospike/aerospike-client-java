/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.policy.Policy;

public final class BatchExecutor extends Thread {
	
	private final Policy policy;
	private final BatchNode batchNode;
	private final HashSet<String> binNames;
	private final HashMap<Key,BatchItem> keyMap;
	private final Record[] records;
	private final boolean[] existsArray;
	private final int readAttr;
	private Exception exception;

	public BatchExecutor(
		Policy policy,
		BatchNode batchNode,
		HashMap<Key,BatchItem> keyMap,
		HashSet<String> binNames,
		Record[] records,
		boolean[] existsArray,
		int readAttr
	) {
		this.policy = policy;
		this.batchNode = batchNode;
		this.keyMap = keyMap;
		this.binNames = binNames;
		this.records = records;
		this.existsArray = existsArray;
		this.readAttr = readAttr;
	}
	
	public void run() {
		try {
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {			
				if (records != null) {
					BatchCommandGet command = new BatchCommandGet(batchNode.node, keyMap, binNames, records);
					command.setBatchGet(batchNamespace, binNames, readAttr);
					command.execute(policy);
				}
				else {
					BatchCommandExists command = new BatchCommandExists(batchNode.node, keyMap, existsArray);
					command.setBatchExists(batchNamespace);
					command.execute(policy);
				}
			}
		}
		catch (Exception e) {
			exception = e;
		}
	}
	
	public Exception getException() {
		return exception;
	}

	public static void executeBatch
	(
		Cluster cluster,
		Policy policy, 
		Key[] keys,
		boolean[] existsArray, 
		Record[] records, 
		HashSet<String> binNames,
		int readAttr
	) throws AerospikeException {
		
		HashMap<Key,BatchItem> keyMap = BatchItem.generateMap(keys);
		List<BatchNode> batchNodes = BatchNode.generateList(cluster, keys);
		
		// Dispatch the work to each node on a different thread.
		ArrayList<BatchExecutor> threads = new ArrayList<BatchExecutor>(batchNodes.size());

		for (BatchNode batchNode : batchNodes) {
			BatchExecutor thread = new BatchExecutor(policy, batchNode, keyMap, binNames, records, existsArray, readAttr);
			threads.add(thread);
			thread.start();
		}
		
		// Wait for all the threads to finish their work and return results.
		for (BatchExecutor thread : threads) {
			try {
				thread.join();
			}
			catch (Exception e) {
			}
		}
		
		// Throw an exception if an error occurred.
		for (BatchExecutor thread : threads) {
			Exception e = thread.getException();
			
			if (e != null) {
				if (e instanceof AerospikeException) {
					throw (AerospikeException)e;					
				}
				else {
					throw new AerospikeException(e);
				}
			}
		}
	}
}
