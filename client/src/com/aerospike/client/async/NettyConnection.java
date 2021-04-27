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

import java.nio.ByteBuffer;

import com.aerospike.client.Log;
import com.aerospike.client.util.Util;

import io.netty.channel.socket.SocketChannel;

/**
 * Aerospike wrapper around netty channel.
 * Implements the Aerospike AsyncConnection interface.
 */
public final class NettyConnection extends AsyncConnection {

	final SocketChannel channel;

	/**
	 * Construct Aerospike channel wrapper from netty channel.
	 */
	public NettyConnection(SocketChannel channel) {
		this.channel = channel;
	}

	/**
	 * Validate connection in a transaction.
	 */
	@Override
	public boolean isValid(ByteBuffer notUsed) {
		return channel.isActive();
	}

	/**
	 * Is connection open.
	 */
	public boolean isOpen() {
		return channel.isOpen();
	}

	/**
	 * Close connection.
	 */
	@Override
	public void close() {
		try {
			channel.close();
		}
		catch (Throwable e) {
			if (Log.debugEnabled()) {
				Log.debug("Error closing socket: " + Util.getErrorMessage(e));
			}
		}
	}
}
