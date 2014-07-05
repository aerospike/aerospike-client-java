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

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.util.Util;

public final class SelectorManager extends Thread implements Closeable {
    private final ConcurrentLinkedQueue<AsyncCommand> commandQueue = new ConcurrentLinkedQueue<AsyncCommand>();
    private final ArrayDeque<AsyncCommand> timeoutQueue;
    private final Selector selector;
    private final ExecutorService taskThreadPool;
    private final AtomicBoolean awakened = new AtomicBoolean();
    private final long selectorTimeout;
	private volatile boolean valid;
    
    public SelectorManager(AsyncClientPolicy policy, SelectorProvider provider) throws IOException {
    	this.selectorTimeout = policy.asyncSelectorTimeout;
    	this.taskThreadPool = policy.asyncTaskThreadPool;
    	selector = provider.openSelector();
    	timeoutQueue = new ArrayDeque<AsyncCommand>(policy.asyncMaxCommands);
    }
    
    public void execute(AsyncCommand command) {
    	commandQueue.add(command);
    	
        if (awakened.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    public void run() {
    	valid = true;
    	
		while (valid) {			
			try {
				runCommands();
			}
			catch (Exception e) {
				if (valid) {
					if (Log.warnEnabled()) {
						Log.warn("Event manager error: " + Util.getErrorMessage(e));
					}				
	                // Backoff when unexpected errors occur.
					Util.sleep(1000);
				}
			}
		}
    }
    
    private void runCommands() throws Exception {
    	checkTimeouts();
    	registerCommands();
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
	        Iterator<SelectionKey> iter = keys.iterator();
	        
	        while (valid && iter.hasNext()) {
	        	SelectionKey key = iter.next();

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
    	AsyncCommand command;
    	
    	while ((command = commandQueue.poll()) != null) {
	    	try {
	    		if (command.timeout > 0) {
		    		if (command.checkTimeout()) {
		    			timeoutQueue.addLast(command);
		    		}
		    		else {
		    			continue;
		    		}
	    		}	    		
		    	command.conn.register(command, selector);
	    	}
    		catch (Exception e) {
            	command.retryAfterInit(new AerospikeException(e));
    		}	    	
    	}    	
    }

    private void checkTimeouts() {
    	AsyncCommand last = timeoutQueue.peekLast();
    	AsyncCommand command;
   	
    	while ((command = timeoutQueue.pollFirst()) != null) {
    		if (command.checkTimeout()) {
    			timeoutQueue.addLast(command);
    		}
    		
    		if (command == last) {
    			break;
    		}
    	}
    }

    private void processKey(SelectionKey key) {
		AsyncCommand command = (AsyncCommand)key.attachment();

		try {
        	int ops = key.readyOps();
    	
        	if ((ops & SelectionKey.OP_READ) != 0) {        		
        		if (taskThreadPool != null) {
        			key.interestOps(0);
        			taskThreadPool.execute(command);
        		}
        		else {
        			command.read();
        		}
        	}
        	else if ((ops & SelectionKey.OP_WRITE) != 0) {
        		command.write();
        	}
        	else if ((ops & SelectionKey.OP_CONNECT) != 0) {
        		SocketChannel socketChannel = (SocketChannel)key.channel();
        		socketChannel.finishConnect();
        		key.interestOps(SelectionKey.OP_WRITE);
        	}
        }
        catch (AerospikeException.Connection ac) {
        	command.retryAfterInit(ac);
        }
        catch (AerospikeException ae) {
			// Fail without retry on non-network errors.
			command.failOnApplicationError(ae);
        }
        catch (IOException ioe) {
        	command.retryAfterInit(new AerospikeException(ioe));
        }
        catch (Exception e) {
			// Fail without retry on unknown errors.
			command.failOnApplicationError(new AerospikeException(e));
        }
    }
 	
	public void close() {
		if (valid) {
			valid = false;
			interrupt();
			
	        try {
	    		selector.close();
	        } 
	        catch (Exception e) {
	        }
	        
	        if (taskThreadPool != null) {
	        	taskThreadPool.shutdownNow();
	        }
		}
	}	
}
