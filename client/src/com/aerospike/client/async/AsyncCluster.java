/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.NodeValidator;

public final class AsyncCluster extends Cluster {
	// ByteBuffer pool used in asynchronous SocketChannel communications.
	private final BufferQueue bufferQueue;
	
	// Asynchronous network selectors.
	private final SelectorManagers selectorManagers;
	
	// Maximum number of concurrent asynchronous commands.
	private final int maxCommands;
	
	public AsyncCluster(AsyncClientPolicy policy, Host[] hosts) throws AerospikeException {
		super(policy, hosts);
		maxCommands = policy.asyncMaxCommands;
		
		switch (policy.asyncMaxCommandAction) {
		case ACCEPT:
			bufferQueue = new AcceptBufferQueue();
			break;
			
		case REJECT:
			bufferQueue = new RejectBufferQueue(maxCommands);
			break;
			
		case BLOCK:
		default:
			bufferQueue = new BlockBufferQueue(maxCommands);
			break;
		}
		
		selectorManagers = new SelectorManagers(policy);
		initTendThread();
	}
	
	@Override
	protected Node createNode(NodeValidator nv) {
		return new AsyncNode(this, nv);
	}

	public ByteBuffer getByteBuffer() throws AerospikeException {
		return bufferQueue.getByteBuffer();
	}
	
	public void putByteBuffer(ByteBuffer byteBuffer) {
		bufferQueue.putByteBuffer(byteBuffer);
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
	
	private static interface BufferQueue {
		public ByteBuffer getByteBuffer() throws AerospikeException;
		public void putByteBuffer(ByteBuffer byteBuffer);
	}
	
	/**
	 * Block buffer queue is bounded and blocks until a buffer
	 * becomes available.  This queue is a useful throttle to avoid
	 * concurrent asynchronous commands overwhelming the client.
	 */
	private static final class BlockBufferQueue implements BufferQueue {
		private final ArrayBlockingQueue<ByteBuffer> bufferQueue;

		private BlockBufferQueue(int maxCommands) {		
			// Preallocate byteBuffers.
			bufferQueue = new ArrayBlockingQueue<ByteBuffer>(maxCommands);		
			for (int i = 0; i < maxCommands; i++) {
				bufferQueue.add(ByteBuffer.allocateDirect(8192));
			}
		}
		
		@Override
		public ByteBuffer getByteBuffer() throws AerospikeException {			
			try {
				// Wait until byteBuffer becomes available.
				return bufferQueue.take();
			}
			catch (InterruptedException ie) {
				throw new AerospikeException("Buffer pool take interrupted.");
			}
		}
		
		@Override
		public void putByteBuffer(ByteBuffer byteBuffer) {
			bufferQueue.offer(byteBuffer);
		}
	}

	/**
	 * Reject buffer queue is bounded, but does not block.  
	 * Commands are rejected when all buffers are being used.
	 */
	private static final class RejectBufferQueue implements BufferQueue {
		private final ArrayBlockingQueue<ByteBuffer> bufferQueue;

		private RejectBufferQueue(int maxCommands) {		
			// Preallocate byteBuffers.
			bufferQueue = new ArrayBlockingQueue<ByteBuffer>(maxCommands);		
			for (int i = 0; i < maxCommands; i++) {
				bufferQueue.add(ByteBuffer.allocateDirect(8192));
			}
		}
		
		@Override
		public ByteBuffer getByteBuffer() throws AerospikeException {			
			// Check if byteBuffer is available.
			ByteBuffer byteBuffer = bufferQueue.poll();
			if (byteBuffer == null) {
				// Reject command when byteBuffer not available.
				throw new AerospikeException.CommandRejected();
			}
			return byteBuffer;
		}
		
		@Override
		public void putByteBuffer(ByteBuffer byteBuffer) {
			bufferQueue.offer(byteBuffer);
		}
	}
	
	/**
	 * Accept buffer queue is unbounded and never blocks.  
	 * Buffers are allocated whenever they are not available.
	 * The buffer queue grows indefinitely.
	 * It's critical that users of this queue throttle their
	 * own asynchronous commands when using this queue.
	 */
	private static final class AcceptBufferQueue implements BufferQueue {
		private final ConcurrentLinkedQueue<ByteBuffer> bufferQueue;

		private AcceptBufferQueue() {		
			bufferQueue = new ConcurrentLinkedQueue<ByteBuffer>();		
		}
		
		@Override
		public ByteBuffer getByteBuffer() throws AerospikeException {			
			// Check if byteBuffer is available.
			ByteBuffer byteBuffer = bufferQueue.poll();
			if (byteBuffer == null) {
				// Allocate new buffer when byteBuffer not available.
				return ByteBuffer.allocateDirect(8192);
			}
			return byteBuffer;
		}
		
		@Override
		public void putByteBuffer(ByteBuffer byteBuffer) {
			bufferQueue.offer(byteBuffer);
		}
	}
}
