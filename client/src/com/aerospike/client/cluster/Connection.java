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

import java.net.InetSocketAddress;
import java.net.Socket;

import com.aerospike.client.AerospikeException;

public final class Connection {
	private final Socket socket;
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
	
	public void close() {
		try {
			socket.close();
		}
		catch (Exception e) {
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
		
	public Socket getSocket() {
		return socket;
	}
	
	public void updateLastUsed() {
		lastUsed = System.currentTimeMillis();
	}
}
