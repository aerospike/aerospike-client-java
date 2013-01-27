/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.util.Util;

/**
 * Socket connection wrapper.
 */
public final class Connection {
	private final Socket socket;
	private InputStream in;
	private OutputStream out;
	private long lastUsed;
	
	public Connection(InetSocketAddress address, int timeoutMillis) throws AerospikeException.Connection {
		try {
			socket = new Socket();
			socket.setTcpNoDelay(true);
			socket.setSoTimeout(timeoutMillis);
			socket.connect(address, timeoutMillis);
			lastUsed = System.currentTimeMillis();
		}
		catch (Exception e) {
			throw new AerospikeException.Connection(e);
		}
	}

	public boolean isConnected() {
		return socket.isConnected();
	}

	/**
	 * Is socket connected and used within the last 14 seconds.
	 */
	public boolean isValid() {
		return socket.isConnected() && (System.currentTimeMillis() - lastUsed) <= 14000;
	}

	public void setTimeout(int timeout) throws SocketException {
		socket.setSoTimeout(timeout);
	}
	
	public InputStream getInputStream() throws IOException {
		in = socket.getInputStream();
		return in;
	}

	public OutputStream getOutputStream() throws IOException {
		out = socket.getOutputStream();
		return out;
	}
		
	public void updateLastUsed() {
		lastUsed = System.currentTimeMillis();
	}
	
	/**
	 * Close socket and associated streams.
	 */
	public void close() {
		try {
			if (in != null) {
				in.close();
				in = null;
			}
			
			if (out != null) {
				out.close();
				out = null;
			}
			socket.close();
		}
		catch (Exception e) {
			if (Log.debugEnabled()) {
				Log.debug("Error closing socket: " + Util.getErrorMessage(e));
			}
		}
	}
}
