/*
 * Copyright 2012-2023 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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

/**
 * Pool for byte[] buffers.
 */
public final class BufferPool {
	/**
	 * Singleton instance.
	 */
	public static BufferPool Instance = new BufferPool();

	/**
	 * Default buffer size returned by the pool.
	 */
	public static int DefaultSize = 8 * 1024; // 8 KB

	/**
	 * Maximum buffer size in the pool.
	 */
	public static int MaxSize = 128 * 1024; // 128 KB

	/**
	 * Get buffer with default size.
	 */
	public byte[] get() {
		return get(DefaultSize);
	}

	/**
	 * Get buffer with specified size.
	 */
	public byte[] get(int size) {
		// Do not store extremely large buffers in buffer pool.
		if (size > MaxSize) {
			if (Log.debugEnabled()) {
				Log.debug("Allocate buffer on heap " + size);
			}
			return new byte[size];
		}

		if (Log.debugEnabled()) {
			Log.debug("Resize buffer to " + size);
		}
		// TODO: Support real buffer pool.
		// For now, just allocate on heap.
		return new byte[size];
	}

	/**
	 * Put buffer back into pool.
	 */
	public void put(byte[] buffer) {
		try {
			if (buffer.length <= BufferPool.MaxSize) {
				// TODO: Support real buffer pool.
				// Make sure put() never throws an exception.
			}
		}
		catch (Throwable t) {
			if (Log.errorEnabled()) {
				Log.error("Buffer pool put error: " + Util.getErrorMessage(t));
			}
		}
	}
}
