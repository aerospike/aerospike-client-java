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
package com.aerospike.client.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.util.Util;

/**
 * Aerospike wrapper around NIO Selector.
 * Implements the Aerospike EventLoop interface.
 */
public final class NioEventLoop extends EventLoopBase implements Runnable {

	final ConcurrentLinkedDeque<Runnable> commandQueue;
	final ArrayDeque<ScheduleTask> scheduleQueue;
	final ArrayDeque<ByteBuffer> byteBufferQueue;
	final Selector selector;
	final AtomicBoolean awakened = new AtomicBoolean();
	final Thread thread;
	final long selectorTimeout;

    /**
     * Construct Aerospike event loop wrapper from NIO Selector.
     */
	public NioEventLoop(EventPolicy policy, SelectorProvider provider, int index) throws IOException {
		super(policy, index);

		commandQueue = new ConcurrentLinkedDeque<Runnable>();		
		scheduleQueue = new ArrayDeque<ScheduleTask>(8);
		byteBufferQueue = new ArrayDeque<ByteBuffer>(policy.commandsPerEventLoop);
		selectorTimeout = policy.minTimeout;
		selector = provider.openSelector();

		thread = new Thread(this, "event" + index);
		thread.setDaemon(false);
	}

	/**
	 * Execute async command.  Execute immediately if in event loop.
	 * Otherwise, place command on event loop queue.
	 */
	@Override
	public void execute(Cluster cluster, AsyncCommand command) {
		new NioCommand(this, cluster, command);
	}

	/**
	 * Schedule execution of runnable command on event loop.
	 * Command is placed on event loop queue and is never executed directly.
	 */
	@Override
	public void execute(Runnable command) {
		commandQueue.offerLast(command);
		
		if (awakened.compareAndSet(false, true)) {
			selector.wakeup();
		}
	}

	/**
	 * Schedule execution of runnable command with delay in milliseconds.
	 */
	@Override
	public void schedule(Runnable command, long delay, TimeUnit unit) {
		final ScheduleTask task = new ScheduleTask(command, delay, unit);
		
		if (thread == Thread.currentThread()) {
			scheduleQueue.offer(task);
		}
		else {
			execute(new Runnable() {
				public void run() {
					scheduleQueue.offer(task);
				}
			});
		}
	}
	
	/**
	 * Schedule execution with a reusable ScheduleTask.
	 * Saves memory allocation for repeatedly scheduled task.
	 */
	@Override
	public void schedule(final ScheduleTask task, long delay, TimeUnit unit) {
		task.setDeadline(delay, unit);

		if (thread == Thread.currentThread()) {
			scheduleQueue.offer(task);
		}
		else {
			execute(new Runnable() {
				public void run() {
					scheduleQueue.offer(task);
				}
			});
		}
	}

	/**
	 * Is current thread the event loop thread.
	 */
	public boolean inEventLoop() {
		return thread == Thread.currentThread();
	}

	public static ByteBuffer createByteBuffer(int size) {
		// Round up to nearest 8KB.		
		return ByteBuffer.allocateDirect((size + 8191) & ~8191);
	}

	public ByteBuffer getByteBuffer() {
		ByteBuffer byteBuffer = byteBufferQueue.pollFirst();
		
		if (byteBuffer == null) {
			byteBuffer = createByteBuffer(8192);
		}
		return byteBuffer;
	}
	
	public void putByteBuffer(ByteBuffer byteBuffer) {
		byteBufferQueue.addLast(byteBuffer);
	}

	public void run() {		
		while (true) {			
			try {
				runCommands();
			}
			catch (CloseException ce) {
				break;
			}
			catch (Exception e) {
				if (Log.warnEnabled()) {
					Log.warn("Event loop error: " + Util.getErrorMessage(e));
				}				
				// Backoff when unexpected errors occur.
				Util.sleep(100);
			}
		}
		close();
	}
	
	private void runCommands() throws Exception {
		registerCommands();
		runScheduled();
		awakened.set(false);
	    selector.select(selectorTimeout);
	    
		if (awakened.get()) {
			selector.wakeup();
		}
	    
		final Set<SelectionKey> keys = selector.selectedKeys();
		
		if (keys.isEmpty()) {
			return;
		}
		
		try {
			final Iterator<SelectionKey> iter = keys.iterator();
			
			while (iter.hasNext()) {
				final SelectionKey key = iter.next();
			
				if (! key.isValid()) {
					continue;
				}	        	
				processKey(key);
			}
		}
		finally {
			keys.clear();
		}
	}
    
    private void registerCommands() {
		Runnable last = commandQueue.peekLast();
		Runnable command;
			
		while ((command = commandQueue.pollFirst()) != null) {
			command.run();

			if (command == last) {
				break;
			}
		}
	}

	private void runScheduled() {
		ScheduleTask last = scheduleQueue.peekLast();
		ScheduleTask command;
		long currentTime = System.nanoTime();

		while ((command = scheduleQueue.pollFirst()) != null) {
			if (command.deadline <= currentTime) {
				command.run();
				currentTime = System.nanoTime();
			}
			else {
				scheduleQueue.addLast(command);
			}

			if (command == last) {
				break;
			}
		}
	}

    private void processKey(SelectionKey key) {
		NioCommand command = (NioCommand)key.attachment();

		try {
        	int ops = key.readyOps();
    	
        	if ((ops & SelectionKey.OP_READ) != 0) {
        		command.read();
        	}
        	else if ((ops & SelectionKey.OP_WRITE) != 0) {
        		command.write();
        	}
        	else if ((ops & SelectionKey.OP_CONNECT) != 0) {
        		command.finishConnect();
        	}
        }
        catch (AerospikeException.Connection ac) {
        	command.onNetworkError(ac, false);
        }
        catch (AerospikeException ae) {
        	if (ae.getResultCode() == ResultCode.TIMEOUT) {
        		// Go through retry logic on server timeout
        		command.onServerTimeout();
        	}
        	else {
        		command.onApplicationError(ae);
        	}
        }
        catch (IOException ioe) {
        	command.onNetworkError(new AerospikeException(ioe), false);
        }
        catch (Exception e) {
			command.onApplicationError(new AerospikeException(e));
        }
    }

	public void close() {
		try {
			selector.close();
		}
		catch (Exception e) {
		}
	}

	static class CloseException extends RuntimeException {
		private static final long serialVersionUID = 1L;
	}
}
