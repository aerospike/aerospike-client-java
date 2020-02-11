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
package com.aerospike.test;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.async.TestAsyncBatch;
import com.aerospike.test.async.TestAsyncOperate;
import com.aerospike.test.async.TestAsyncPutGet;
import com.aerospike.test.async.TestAsyncQuery;
import com.aerospike.test.async.TestAsyncScan;
import com.aerospike.test.async.TestAsyncUDF;
import com.aerospike.test.util.Args;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestAsyncPutGet.class,
	TestAsyncBatch.class,
	TestAsyncOperate.class,
	TestAsyncScan.class,
	TestAsyncQuery.class,
	TestAsyncUDF.class
})
public class SuiteAsync {
	public static AerospikeClient client = null;
	public static EventLoops eventLoops = null;
	public static EventLoop eventLoop = null;

	@BeforeClass
	public static void init() {
		System.out.println("Begin AerospikeClient");
		Args args = Args.Instance;

		switch (args.eventLoopType) {
			default:
			case DIRECT_NIO: {
				eventLoops = new NioEventLoops(1);
				break;
			}

			case NETTY_NIO: {
				EventLoopGroup group = new NioEventLoopGroup(1);
				eventLoops = new NettyEventLoops(group);
				break;
			}

			case NETTY_EPOLL: {
				EventLoopGroup group = new EpollEventLoopGroup(1);
				eventLoops = new NettyEventLoops(group);
				break;
			}
		}

		try {
			ClientPolicy policy = new ClientPolicy();
			policy.eventLoops = eventLoops;
			policy.user = args.user;
			policy.password = args.password;
			policy.authMode = args.authMode;
			policy.tlsPolicy = args.tlsPolicy;

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
