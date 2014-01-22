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
	//private static final int MAX_BUFFER_SIZE = 1024 * 1024;  // 1 MB
	private static final int THREAD_LOCAL_CUTOFF = 1024 * 128;  // 128 KB
	
	private static final ThreadLocal<byte[]> BufferThreadLocal = new ThreadLocal<byte[]>() {
		@Override protected byte[] initialValue() {
			return new byte[8192];
		}
	};
		
	public static byte[] getBuffer() {
		return BufferThreadLocal.get();
	}
	
	public static byte[] resizeBuffer(int size) {
		// Do not store extremely large buffers in thread local storage.
		if (size > THREAD_LOCAL_CUTOFF) {
			/*
			if (size > MAX_BUFFER_SIZE) {
				throw new IllegalArgumentException("Thread " + Thread.currentThread().getId() + " invalid buffer size: " + size);
			}*/
			
			if (Log.debugEnabled()) {
				Log.debug("Thread " + Thread.currentThread().getId() + " allocate buffer on heap " + size);
			}
			return new byte[size];		
		}

		if (Log.debugEnabled()) {
			Log.debug("Thread " + Thread.currentThread().getId() + " resize buffer to " + size);
		}
		BufferThreadLocal.set(new byte[size]);
		return BufferThreadLocal.get();
	}	
}
