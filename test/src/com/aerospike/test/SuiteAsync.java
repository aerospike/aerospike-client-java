/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.async.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.async.*;
import com.aerospike.test.util.Args;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueIoHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.uring.IoUringIoHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestAsyncPutGet.class,
	TestAsyncBatch.class,
	TestAsyncOperate.class,
	TestAsyncScan.class,
	TestAsyncQuery.class,
	TestAsyncTxn.class,
	TestAsyncUDF.class,
	TestAsyncErrorDetailVerbosity.class
})
public class SuiteAsync {
	public static IAerospikeClient client = null;
	public static EventLoops eventLoops = null;
	public static EventLoop eventLoop = null;

	@BeforeClass
	public static void init() {
		Log.setCallback(null);

		System.out.println("Begin AerospikeClient");
		Args args = Args.Instance;

		EventPolicy eventPolicy = new EventPolicy();

		switch (args.eventLoopType) {
			default:
			case DIRECT_NIO: {
				eventLoops = new NioEventLoops(eventPolicy, 1);
				break;
			}

			case NETTY_NIO: {
                EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
				eventLoops = new NettyEventLoops(eventPolicy, group, args.eventLoopType);
				break;
			}

			case NETTY_EPOLL: {
                EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
				eventLoops = new NettyEventLoops(eventPolicy, group, args.eventLoopType);
				break;
			}

			case NETTY_KQUEUE: {
                EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, KQueueIoHandler.newFactory());
				eventLoops = new NettyEventLoops(eventPolicy, group, args.eventLoopType);
				break;
			}

			case NETTY_IOURING: {
                EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
				eventLoops = new NettyEventLoops(eventPolicy, group, args.eventLoopType);
				break;
			}
		}

		try {
			ClientPolicy policy = new ClientPolicy();
			args.setClientPolicy(policy);
			policy.eventLoops = eventLoops;

			Host[] hosts = Host.parseHosts(args.host, args.port);

			eventLoop = eventLoops.get(0);

			client = new AerospikeClient(policy, hosts);

			try {
				args.setServerSpecific(client);
			}
			catch (RuntimeException re) {
				client.close();
				throw re;
			}
		}
		catch (Exception e) {
			eventLoops.close();
			throw e;
		}
	}

	@AfterClass
	public static void destroy() {
		System.out.println("End AerospikeClient");
		if (client != null) {
			client.close();
		}

		if (eventLoops != null) {
			eventLoops.close();
		}
	}
}
