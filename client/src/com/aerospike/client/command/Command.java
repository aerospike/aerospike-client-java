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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

public abstract class Command {
	// Flags commented out are not supported by this client.
	public static final int INFO1_READ				= (1 << 0); // Contains a read operation.
	public static final int INFO1_GET_ALL			= (1 << 1); // Get all bins.
	//public static final int INFO1_GET_ALL_NODATA 	= (1 << 2); // Get all bins WITHOUT data (currently unimplemented).
	//public static final int INFO1_VERIFY			= (1 << 3); // Verify is a GET transaction that includes data.
	//public static final int INFO1_XDS				= (1 << 4); // Operation is being performed by XDS
	public static final int INFO1_NOBINDATA			= (1 << 5); // Do not read the bins

	public static final int INFO2_WRITE				= (1 << 0); // Create or update record
	public static final int INFO2_DELETE			= (1 << 1); // Fling a record into the belly of Moloch.
	public static final int INFO2_GENERATION		= (1 << 2); // Update if expected generation == old.
	public static final int INFO2_GENERATION_GT		= (1 << 3); // Update if new generation >= old, good for restore.
	public static final int INFO2_GENERATION_DUP	= (1 << 4); // Create a duplicate on a generation collision.
	public static final int INFO2_WRITE_UNIQUE		= (1 << 5); // Fail if record already exists.
	//private final int INFO2_WRITE_BINUNIQUE		= (1 << 6);

	public static final int INFO3_LAST				= (1 << 0); // this is the last of a multi-part message
	//public static final int  INFO3_TRACE			= (1 << 1); // apply server trace logging for this transaction
	//public static final int  INFO3_TOMBSTONE		= (1 << 2); // if set on response, a version was a delete tombstone	
			
	public final static int DIGEST_SIZE = 20;
	
	public static final int MSG_TIMEOUT_OFFSET = 22;
	public static final int MSG_REMAINING_HEADER_SIZE = 22;
	public static final int MSG_TOTAL_HEADER_SIZE = 30;
	public static final int FIELD_HEADER_SIZE = 5;
	public static final int OPERATION_HEADER_SIZE = 8;
	public static final long CL_MSG_VERSION = 2L;
	public static final long AS_MSG_TYPE = 3L;

	protected byte[] sendBuffer;
	protected byte[] receiveBuffer;
	protected int sendOffset;

	public Command() {
		this.sendOffset = MSG_TOTAL_HEADER_SIZE;
	}
	
	public final void estimateOperationSize(Bin bin) throws AerospikeException {
		sendOffset += Buffer.estimateSizeUtf8(bin.name) + OPERATION_HEADER_SIZE;
		sendOffset += bin.value.estimateSize();
	}

	public final void estimateOperationSize(Operation operation) throws AerospikeException {
		sendOffset += Buffer.estimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
		sendOffset += operation.binValue.estimateSize();
	}

	public final void estimateOperationSize(String binName) {
		sendOffset += Buffer.estimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
	}

	public final void estimateOperationSize() {
		sendOffset += OPERATION_HEADER_SIZE;
	}
	
	public final void writeOperation(Bin bin, Operation.Type operation) throws AerospikeException {
        int nameLength = Buffer.stringToUtf8(bin.name, sendBuffer, sendOffset + OPERATION_HEADER_SIZE);
        int valueLength = bin.value.write(sendBuffer, sendOffset + OPERATION_HEADER_SIZE + nameLength);
         
        Buffer.intToBytes(nameLength + valueLength + 4, sendBuffer, sendOffset);
		sendOffset += 4;
        sendBuffer[sendOffset++] = (byte) operation.protocolType;
        sendBuffer[sendOffset++] = (byte) bin.value.getType();
        sendBuffer[sendOffset++] = (byte) 0;
        sendBuffer[sendOffset++] = (byte) nameLength;
        sendOffset += nameLength + valueLength;
	}
		
	public final void writeOperation(Operation operation) throws AerospikeException {
        int nameLength = Buffer.stringToUtf8(operation.binName, sendBuffer, sendOffset + OPERATION_HEADER_SIZE);
        int valueLength = operation.binValue.write(sendBuffer, sendOffset + OPERATION_HEADER_SIZE + nameLength);
         
        Buffer.intToBytes(nameLength + valueLength + 4, sendBuffer, sendOffset);
		sendOffset += 4;
        sendBuffer[sendOffset++] = (byte) operation.type.protocolType;
        sendBuffer[sendOffset++] = (byte) operation.binValue.getType();
        sendBuffer[sendOffset++] = (byte) 0;
        sendBuffer[sendOffset++] = (byte) nameLength;
        sendOffset += nameLength + valueLength;
	}

	public final void writeOperation(String name, Operation.Type operation) {
        int nameLength = Buffer.stringToUtf8(name, sendBuffer, sendOffset + OPERATION_HEADER_SIZE);
         
        Buffer.intToBytes(nameLength + 4, sendBuffer, sendOffset);
		sendOffset += 4;
        sendBuffer[sendOffset++] = (byte) operation.protocolType;
        sendBuffer[sendOffset++] = (byte) 0;
        sendBuffer[sendOffset++] = (byte) 0;
        sendBuffer[sendOffset++] = (byte) nameLength;
        sendOffset += nameLength;
	}

	public final void writeOperation(Operation.Type operation) {
        sendBuffer[sendOffset++] = 0;
        sendBuffer[sendOffset++] = 0;
        sendBuffer[sendOffset++] = 0;
        sendBuffer[sendOffset++] = 0;
        sendBuffer[sendOffset++] = (byte) operation.protocolType;
        sendBuffer[sendOffset++] = 0;
        sendBuffer[sendOffset++] = 0;
        sendBuffer[sendOffset++] = 0;
	}

	public final void writeField(String str, int type) {
		int len = Buffer.stringToUtf8(str, sendBuffer, sendOffset + FIELD_HEADER_SIZE);
		writeFieldHeader(len, type);
		sendOffset += len;
	}
		
	public final void writeField(byte[] bytes, int type) throws AerospikeException {
	    System.arraycopy(bytes, 0, sendBuffer, sendOffset + FIELD_HEADER_SIZE, bytes.length);
	    writeFieldHeader(bytes.length, type);
		sendOffset += bytes.length;
	}

	public final void writeFieldHeader(int size, int type) {
		Buffer.intToBytes(size+1, sendBuffer, sendOffset);
		sendOffset += 4;
		sendBuffer[sendOffset++] = (byte)type;
	}
	
	public final void execute(Policy policy) throws AerospikeException {
		// Write total size of message which is the current offset.
		long size = (sendOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
		Buffer.longToBytes(size, sendBuffer, 0);

		if (policy == null) {
			policy = new Policy();
		}
		        
		int maxIterations = policy.maxRetries + 1;
		int remainingMillis = policy.timeout;
		long limit = System.currentTimeMillis() + remainingMillis;
        int failedNodes = 0;
        int failedConns = 0;
        int i;

        // Execute command until successful, timed out or maximum iterations have been reached.
		for (i = 0; i < maxIterations; i++) {
			Node node = null;
			try {		
				node = getNode();
				Connection conn = node.getConnection(remainingMillis);
				
				try {
					// Reset timeout in send buffer (destined for server) and socket.
					Buffer.intToBytes(remainingMillis, sendBuffer, MSG_TIMEOUT_OFFSET);
					
					// Send command.
					send(conn);
					
					// Parse results.
					parseResult(conn.getInputStream());
					
					// Reflect healthy status.
					conn.updateLastUsed();
					node.restoreHealth();
					
					// Put connection back in pool.
					node.putConnection(conn);
					
					// Command has completed successfully.  Exit method.
					return;
				}
				catch (AerospikeException ae) {
					// Close socket to flush out possible garbage.  Do not put back in pool.
					conn.close();
					throw ae;
				}
				catch (RuntimeException re) {
					// All runtime exceptions are considered fatal.  Do not retry.
					// Close socket to flush out possible garbage.  Do not put back in pool.
					conn.close();
					throw re;
				}
				catch (IOException ioe) {
					// IO errors are considered temporary anomalies.  Retry.
					// Close socket to flush out possible garbage.  Do not put back in pool.
					conn.close();
					
					if (Log.debugEnabled()) {
						Log.debug("Node " + node + ": " + Util.getErrorMessage(ioe));
					}
					// IO error means connection to server node is unhealthy.
					// Reflect this status.
					node.decreaseHealth(60);
				}
			}
			catch (AerospikeException.InvalidNode ine) {
				// Node is currently inactive.  Retry.
				failedNodes++;
			}
			catch (AerospikeException.Connection ce) {
				// Socket connection error has occurred. Decrease health and retry.
				node.decreaseHealth(60);
				
				if (Log.debugEnabled()) {
					Log.debug("Node " + node + ": " + Util.getErrorMessage(ce));
				}
				failedConns++;	
			}

			// Check for client timeout.
			if (policy.timeout > 0) {
				remainingMillis = (int)(limit - System.currentTimeMillis());

				if (remainingMillis <= 0) {
					break;
				}
			}
			// Sleep before trying again.
			Util.sleep(policy.sleepBetweenRetries);
		}
		
		if (Log.debugEnabled()) {
			Log.debug("Client timeout: timeout=" + policy.timeout + " iterations=" + i + 
				" failedNodes=" + failedNodes + " failedConns=" + failedConns);
		}
		throw new AerospikeException.Timeout();
	}
	
	private final void send(Connection conn) throws IOException {
		final OutputStream os = conn.getOutputStream();
		
		// Never write more than 8 KB at a time.  Apparently, the jni socket write does an extra 
		// malloc and free if buffer size > 8 KB.
		final int max = sendOffset;
		int pos = 0;
		int len;
		
		while (pos < max) {
			len = max - pos;
			
			if (len > 8192)
				len = 8192;
			
			os.write(sendBuffer, pos, len);
			pos += len;
		}
	}
	
	public static void readFully(InputStream is, byte[] buf, int length) throws IOException {
		int pos = 0;
	
		while (pos < length) {
			int count = is.read(buf, pos, length - pos);
		    
			if (count < 0)
		    	throw new EOFException();
			
			pos += count;
		}
	}
	
	protected abstract Node getNode() throws AerospikeException.InvalidNode;
	protected abstract void parseResult(InputStream is) throws AerospikeException, IOException;
}
