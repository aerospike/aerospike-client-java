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
package com.aerospike.client.cluster;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.util.Util;

/**
 * Socket connection wrapper.
 */
public final class Connection implements Closeable{
	private final Socket socket;
	private final InputStream in;
	private final OutputStream out;
	private final long maxSocketIdleMillis;
	private long lastUsed;
	
	public Connection(InetSocketAddress address, int timeoutMillis) throws AerospikeException.Connection {
		this(address, timeoutMillis, 14);
	}

	public Connection(InetSocketAddress address, int timeoutMillis, int maxSocketIdleSeconds) throws AerospikeException.Connection {
		this.maxSocketIdleMillis = (long)maxSocketIdleSeconds * 1000L;

		try {
			socket = new Socket();
			socket.setTcpNoDelay(true);
			
			if (timeoutMillis > 0) {
				socket.setSoTimeout(timeoutMillis);
			}
			else {				
				// Do not wait indefinitely on connection if no timeout is specified.
				// Retry functionality will attempt to reconnect later.
				timeoutMillis = 2000;
			}
			socket.connect(address, timeoutMillis);
			in = socket.getInputStream();
			out = socket.getOutputStream();
			lastUsed = System.currentTimeMillis();
		}
		catch (Exception e) {
			throw new AerospikeException.Connection(e);
		}
	}
	
	public void write(byte[] buffer, int length) throws IOException {
		// Never write more than 8 KB at a time.  Apparently, the jni socket write does an extra 
		// malloc and free if buffer size > 8 KB.
		final int max = length;
		int pos = 0;
		int len;
		
		while (pos < max) {
			len = max - pos;
			
			if (len > 8192)
				len = 8192;
			
			out.write(buffer, pos, len);
			pos += len;
		}
	}
	
	public void readFully(byte[] buffer, int length) throws IOException {
		int pos = 0;
	
		while (pos < length) {
			int count = in.read(buffer, pos, length - pos);
		    
			if (count < 0)
		    	throw new EOFException();
			
			pos += count;
		}
	}

	public boolean isConnected() {
		return socket.isConnected();
	}

	/**
	 * Is socket connected and used within specified limits.
	 */
	public boolean isValid() {
		return socket.isConnected() && (System.currentTimeMillis() - lastUsed) <= maxSocketIdleMillis;
	}

	public void setTimeout(int timeout) throws SocketException {
		socket.setSoTimeout(timeout);
	}
	
	public InputStream getInputStream() {
		return in;
	}
			
	public void updateLastUsed() {
		lastUsed = System.currentTimeMillis();
	}
	
	/**
	 * Close socket and associated streams.
	 */
	public void close() {
		try {
			in.close();
			out.close();			
			socket.close();
		}
		catch (Exception e) {
			if (Log.debugEnabled()) {
				Log.debug("Error closing socket: " + Util.getErrorMessage(e));
			}
		}
	}
}
