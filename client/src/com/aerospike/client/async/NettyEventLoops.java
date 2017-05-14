/*
 * Copyright 2012-2017 Aerospike, Inc.
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

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.EventExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

/**
 * Aerospike wrapper around netty event loops.
 * Implements the Aerospike EventLoops interface.
 */
public final class NettyEventLoops implements EventLoops {

	private final HashMap<EventExecutor,NettyEventLoop> eventLoopMap;
	private final NettyEventLoop[] eventLoopArray;
	private final EventLoopGroup group;
	TlsPolicy tlsPolicy;
	SslContext sslContext;
    private int eventIter;

    /**
     * Create Aerospike event loop wrappers from given netty event loops.
     */
	public NettyEventLoops(EventLoopGroup group) {
		this(new EventPolicy(), group);
	}

    /**
     * Create Aerospike event loop wrappers from given netty event loops.
     */
	public NettyEventLoops(EventPolicy policy, EventLoopGroup group) {
		if (policy.minTimeout < 5) {
			throw new AerospikeException("Invalid minTimeout " + policy.minTimeout + ". Must be at least 5.");
		}
		this.group = group;
		
		ArrayList<NettyEventLoop> list = new ArrayList<NettyEventLoop>();
		Iterator<EventExecutor> iter = group.iterator();
		int count = 0;

		while (iter.hasNext()) {
			EventExecutor eventExecutor = iter.next();
			list.add(new NettyEventLoop(policy, (io.netty.channel.EventLoop)eventExecutor, this, count++));
		}

		eventLoopArray = list.toArray(new NettyEventLoop[count]);
		eventLoopMap = new HashMap<EventExecutor,NettyEventLoop>(count);
		
		for (NettyEventLoop eventLoop : eventLoopArray) {
			eventLoopMap.put(eventLoop.eventLoop, eventLoop);
		}
	}
	
	/**
	 * Initialize TLS context. For internal use only.
	 */
	public void initTlsContext(TlsPolicy policy) {
		if (this.tlsPolicy != null) {
			// Already initialized.
			return;
		}

		this.tlsPolicy = policy;
		
		try {
			SslContextBuilder builder = SslContextBuilder.forClient();
			
			if (policy.protocols != null) {
				builder.protocols(policy.protocols);
			}
			
			if (policy.ciphers != null) {
				builder.ciphers(Arrays.asList(policy.ciphers));
			}
			sslContext = builder.build();
		}
		catch (Exception e) {
			throw new AerospikeException("Failed to init netty TLS: " + Util.getErrorMessage(e));
		}
	}

	/**
	 * Return corresponding Aerospike event loop given netty event loop.
	 */
	public NettyEventLoop get(EventExecutor eventExecutor) {
		return eventLoopMap.get(eventExecutor);
	}
	
   /**
     * Return array of Aerospike event loops.
     */
	@Override
	public NettyEventLoop[] getArray() {
		return eventLoopArray;
	}
	
    /**
     * Return number of event loops in this group.
     */
	@Override
	public int getSize() {
		return eventLoopArray.length;
	}

	/**
	 * Return Aerospike event loop given array index..
	 */
	@Override
	public NettyEventLoop get(int index) {
		return eventLoopArray[index];
	}

	/**
	 * Return next Aerospike event loop in round-robin fashion.
	 */
	@Override
	public NettyEventLoop next() {
		int iter = eventIter++; // Not atomic by design
		iter = iter % eventLoopArray.length;
		
		if (iter < 0) {
			iter += eventLoopArray.length;
		}
        return eventLoopArray[iter];
	}
	
	@Override
	public void close() {
		group.shutdownGracefully();
		/*
		for (NettyEventLoop el : eventLoopArray) {
			el.timer.printRemaining();
		}*/
	}
}
