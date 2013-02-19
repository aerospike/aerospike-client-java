/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.util;

import com.aerospike.client.Log;

public final class ThreadLocalData {
	private static final int MAX_BUFFER_SIZE = 1024 * 1024;  // 1 MB
	private static final int THREAD_LOCAL_CUTOFF = 1024 * 128;  // 128 KB
	
	private static final ThreadLocal<byte[]> SendBufferThreadLocal = new ThreadLocal<byte[]>() {
		@Override protected byte[] initialValue() {
			return new byte[2048];
		}
	};
	
	private static final ThreadLocal<byte[]> ReceiveBufferThreadLocal = new ThreadLocal<byte[]>() {
		@Override protected byte[] initialValue() {
			return new byte[2048];
		}
	};
	
	public static byte[] getSendBuffer() {
		return SendBufferThreadLocal.get();
	}
	
	public static byte[] resizeSendBuffer(int size) {
		// Do not store extremely large buffers in thread local storage.
		if (size > THREAD_LOCAL_CUTOFF) {
			if (size > MAX_BUFFER_SIZE) {
				throw new IllegalArgumentException("Thread " + Thread.currentThread().getId() + " invalid send buffer size: " + size);
			}
			
			if (Log.debugEnabled()) {
				Log.debug("Thread " + Thread.currentThread().getId() + " allocate send buffer on heap " + size);
			}
			return new byte[size];		
		}

		if (Log.debugEnabled()) {
			Log.debug("Thread " + Thread.currentThread().getId() + " resize send buffer to " + size);
		}
		SendBufferThreadLocal.set(new byte[size]);
		return SendBufferThreadLocal.get();
	}
	
	public static byte[] getReceiveBuffer() {
		return ReceiveBufferThreadLocal.get();
	}
	
	public static byte[] resizeReceiveBuffer(int size) {
		// Do not store extremely large buffers in thread local storage.
		if (size > THREAD_LOCAL_CUTOFF) {
			if (size > MAX_BUFFER_SIZE) {
				throw new IllegalArgumentException("Thread " + Thread.currentThread().getId() + " invalid receive buffer size: " + size);
			}
			
			if (Log.debugEnabled()) {
				Log.debug("Thread " + Thread.currentThread().getId() + " allocate receive buffer on heap " + size);
			}
			return new byte[size];		
		}

		if (Log.debugEnabled()) {
			Log.debug("Thread " + Thread.currentThread().getId() + " resize receive buffer to " + size);
		}
		ReceiveBufferThreadLocal.set(new byte[size]);
		return ReceiveBufferThreadLocal.get();
	}
}
