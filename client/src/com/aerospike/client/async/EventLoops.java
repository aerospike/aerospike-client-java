/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client.async;

import java.io.Closeable;

import com.aerospike.client.policy.ClientPolicy;

/**
 * Aerospike event loops interface.
 */
public interface EventLoops extends Closeable {
	/**
	 * Return array of Aerospike event loops.
	 */
	public EventLoop[] getArray();

	/**
	 * Return number of event loops in this group.
	 */
	public int getSize();

	/**
	 * Return Aerospike event loop given array index..
	 */
	public EventLoop get(int index);

	/**
	 * Return next Aerospike event loop in round-robin fashion.
	 * Implementations might not use an atomic sequence counter.
	 * Non-atomic counters improve performance, but might result
	 * in a slightly imperfect round-robin distribution.
	 */
	public EventLoop next();

	/**
	 * Close event loops.
	 */
	public void close();

	/**
	 * Initialize event loops with client policy. For internal use only.
	 */
	public void init(ClientPolicy policy);
}
