/*
 * Copyright 2012-2020 Aerospike, Inc.
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

import java.nio.ByteBuffer;

import io.netty.channel.socket.SocketChannel;

/**
 * Aerospike wrapper around netty channel.
 * Implements the Aerospike AsyncConnection interface.
 */
public final class NettyConnection implements AsyncConnection {

	final SocketChannel channel;
	private final long maxSocketIdle;
	private long lastUsed;

    /**
     * Construct Aerospike channel wrapper from netty channel.
     */
	public NettyConnection(SocketChannel channel, long maxSocketIdle) {
		this.channel = channel;
		this.maxSocketIdle = maxSocketIdle;
	}

	/**
	 * Is connection ready for another command.
	 */
	@Override
	public boolean isValid(ByteBuffer notUsed) {
		return (System.nanoTime() - lastUsed) <= maxSocketIdle && channel.isActive();
	}

	/**
	 * Is connection idle time less than or equal to
	 * {@link com.aerospike.client.policy.ClientPolicy#maxSocketIdle}.
	 */
	@Override
	public boolean isCurrent() {
		return (System.nanoTime() - lastUsed) <= maxSocketIdle;
	}

	void updateLastUsed() {
		lastUsed = System.nanoTime();
	}

	/**
	 * Close connection.
	 */
	@Override
	public void close() {
		channel.close();
	}
}
