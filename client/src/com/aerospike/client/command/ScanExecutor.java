package com.aerospike.client.command;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ScanPolicy;

public final class ScanExecutor {
	
	private final ScanPolicy policy;
	private final String namespace;
	private final String setName;
	private final ScanCallback callback;
	private ScanThread[] threads;
	private Exception exception;
	
	public ScanExecutor(ScanPolicy policy, String namespace, String setName, ScanCallback callback) {
		this.policy = policy;
		this.namespace = namespace;
		this.setName = setName;
		this.callback = callback;
	}
	
	public void scanParallel(Node[] nodes)
		throws AerospikeException {
		
		threads = new ScanThread[nodes.length];
		int count = 0;
		
		for (Node node : nodes) {
			ScanThread thread = new ScanThread(this, node);
			threads[count++] = thread;
			thread.start();
		}

		for (ScanThread thread : threads) {
			try {
				thread.join();
			}
			catch (Exception e) {
			}
		}

		// Throw an exception if an error occurred.
		if (exception != null) {
			if (exception instanceof AerospikeException) {
				throw (AerospikeException)exception;		
			}
			else {
				throw new AerospikeException(exception);
			}		
		}		
	}

    private void interrupt(Exception cause)
    {
    	synchronized (this) {
    	   	if (exception != null) {
    	   		return;
    	   	}
	    	exception = cause;  		
    	}
    	
		for (ScanThread thread : threads) {
			try {
				thread.close();
				thread.interrupt();
			}
			catch (Exception e) {
			}
		}
    }

    private static final class ScanThread extends Thread {
		private final ScanExecutor parent;
		private final ScanCommand command;

		public ScanThread(ScanExecutor parent, Node node) {
			this.parent = parent;
			this.command = new ScanCommand(node, parent.callback);
		}
		
		public void run() {
			try {
				command.scan(parent.policy, parent.namespace, parent.setName);
			}
			catch (Exception e) {
				// Terminate other scan threads.
				parent.interrupt(e);
			}
		}
		
	    public void close()
	    {
	    	command.stop();
	    }		
	}
}
