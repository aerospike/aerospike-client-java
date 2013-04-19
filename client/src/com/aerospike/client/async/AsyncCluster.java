/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.async;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.NodeValidator;

public final class AsyncCluster extends Cluster {
	// ByteBuffer pool used in asynchronous SocketChannel communications.
	private final ArrayBlockingQueue<ByteBuffer> bufferQueue;
	
	// Asynchronous network selectors.
	private final SelectorManagers selectorManagers;

	// How to handle cases when the asynchronous maximum number of concurrent database commands 
	// have been exceeded.  
	private final MaxCommandAction maxCommandAction;
	
	// Commands currently used.
	private final AtomicInteger commandsUsed;

	// Maximum number of concurrent asynchronous commands.
	private final int maxCommands;
	
	public AsyncCluster(AsyncClientPolicy policy, Host[] hosts) throws AerospikeException {
		super(policy, hosts);
		maxCommandAction = policy.asyncMaxCommandAction;	
		maxCommands = policy.asyncMaxCommands;
		commandsUsed = new AtomicInteger();
		bufferQueue = new ArrayBlockingQueue<ByteBuffer>(policy.asyncMaxCommands);
		selectorManagers = new SelectorManagers(policy);
		initTendThread();
	}
	
	@Override
	protected Node createNode(NodeValidator nv) {
		return new AsyncNode(this, nv);
	}

	public ByteBuffer getByteBuffer() throws AerospikeException {
		// If buffers available or always accept command, use standard non-blocking poll().
		if (commandsUsed.incrementAndGet() <= maxCommands || maxCommandAction == MaxCommandAction.ACCEPT) {			
			ByteBuffer byteBuffer = bufferQueue.poll();
			
			if (byteBuffer != null) {
				return byteBuffer;
			}
			return ByteBuffer.allocateDirect(8192);
		}
		
		// Max buffers exceeded.  Reject command if specified.
		if (maxCommandAction == MaxCommandAction.REJECT) {
			commandsUsed.decrementAndGet();
			throw new AerospikeException.CommandRejected();
		}
		
		// Block until buffer becomes available.
		try {
			return bufferQueue.take();	
		}
		catch (InterruptedException ie) {
			commandsUsed.decrementAndGet();
			throw new AerospikeException("Buffer pool take interrupted.");
		}
	}
	
	public void putByteBuffer(ByteBuffer byteBuffer) {
		commandsUsed.decrementAndGet();
		bufferQueue.offer(byteBuffer);
	}
	
	public SelectorManager getSelectorManager() {
        return selectorManagers.next();
	}
	
	public int getMaxCommands() {
		return maxCommands;
	}
	
	@Override
	public void close() {
		super.close();		
		selectorManagers.close();
	}
}
