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
