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
