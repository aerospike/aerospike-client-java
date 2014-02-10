/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client.async;

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

public final class SelectorManager extends Thread {
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
