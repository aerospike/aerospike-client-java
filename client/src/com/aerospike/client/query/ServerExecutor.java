package com.aerospike.client.query;

import java.util.Random;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;

public final class ServerExecutor {
	
	private final Policy policy;
	private final Statement statement;
	private ServerThread[] threads;
	private Exception exception;
	
	public ServerExecutor(
		Policy policy,
		Statement statement,
		String packageName, 
		String functionName, 
		Value[] functionArgs
	) {
		this.policy = policy;
		this.statement = statement;
		this.statement.setAggregateFunction(packageName, functionName, functionArgs, false);
		
		if (this.statement.taskId == 0) {
			Random r = new Random();
			this.statement.taskId = r.nextInt(Integer.MAX_VALUE);
		}
	}
	
	public void execute(Node[] nodes)
		throws AerospikeException {
		
		threads = new ServerThread[nodes.length];
		int count = 0;
		
		for (Node node : nodes) {
			ServerCommand command = new ServerCommand(node);
			ServerThread thread = new ServerThread(command);
			threads[count++] = thread;
			thread.start();
		}

		for (ServerThread thread : threads) {
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

    private void stopThreads(Exception cause) {
    	synchronized (this) {
    	   	if (exception != null) {
    	   		return;
    	   	}
	    	exception = cause;  		
    	}
    	
		for (ServerThread thread : threads) {
			try {
				thread.stopThread();
				thread.interrupt();
			}
			catch (Exception e) {
			}
		}
    }

    private final class ServerThread extends Thread {
		private final ServerCommand command;

		public ServerThread(ServerCommand command) {
			this.command = command;
		}
		
		public void run() {
			try {
				command.resetSendBuffer();
				command.query(policy, statement);
			}
			catch (Exception e) {
				// Terminate other threads.
				stopThreads(e);
			}
		}
		
		public void stopThread() {
			command.stop();
		}		
	}
}
