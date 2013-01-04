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

import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ScanPolicy;

public final class ScanThread extends Thread {	
	private final ScanPolicy policy;
	private final Node node;
	private final String namespace;
	private final String setName;
	private final ScanCallback callback;
	private Exception exception;

	public ScanThread(
		ScanPolicy policy,
		Node node,
		String namespace,
		String setName,
		ScanCallback callback
	) {
		this.policy = policy;
		this.node = node;
		this.namespace = namespace;
		this.setName = setName;
		this.callback = callback;
	}
	
	public void run() {
		try {
			ScanCommand command = new ScanCommand(node, callback);
			command.scan(policy, namespace, setName);
		}
		catch (Exception e) {
			exception = e;
		}
	}
	
	public Exception getException() {
		return exception;
	}
}
