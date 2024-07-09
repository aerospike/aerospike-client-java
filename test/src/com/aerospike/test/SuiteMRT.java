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
package com.aerospike.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Log;
// import com.aerospike.client.async.EventLoop;
// import com.aerospike.client.async.EventLoopType;
// import com.aerospike.client.async.EventLoops;
// import com.aerospike.client.async.EventPolicy;
// import com.aerospike.client.async.NettyEventLoops;
// import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.proxy.AerospikeClientFactory;
// import com.aerospike.test.async.TestAsyncTran;

import com.aerospike.test.sync.basic.TestTran;

import com.aerospike.test.util.Args;

// import io.netty.channel.EventLoopGroup;
// import io.netty.channel.epoll.Epoll;
// import io.netty.channel.epoll.EpollEventLoopGroup;
// import io.netty.channel.kqueue.KQueueEventLoopGroup;
// import io.netty.channel.nio.NioEventLoopGroup;
// import io.netty.incubator.channel.uring.IOUringEventLoopGroup;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	// SuiteAsync.class,
	TestTran.class
})
public class SuiteMRT {
	public static IAerospikeClient client = null;

	@BeforeClass
	public static void init() throws InterruptedException {
		Log.setCallback(null);

		System.out.println("Begin AerospikeClient");
		Args args = Args.Instance;

		ClientPolicy policy = new ClientPolicy();
		args.setClientPolicy(policy);

		Host[] hosts = Host.parseHosts(args.host, args.port);

		client = AerospikeClientFactory.getClient(policy, args.useProxyClient, hosts);


        Info.request(null, client.getNodes()[0], "set-config:context=namespace;id=test;strong-consistency-allow-expunge=true");
        System.out.println("Connected Client to DB!");
        // Set Roster to observed_nodes & recluster
        String inf = Info.request(null, client.getNodes()[0], "roster");
        System.out.println("CURRENT ROSTER:");
        System.out.println(inf);
        int prefix_end = inf.lastIndexOf('=');
        String observed = inf.substring(prefix_end + 1);
        System.out.println(observed);
        inf = Info.request(client.getNodes()[0], "roster-set:namespace=test;nodes=" + observed);
        System.out.println("Roster-set? - " + inf);
        for (Node node : client.getNodes() ) {
            inf = Info.request(node, "recluster");
            System.out.println(node.getName() + "-" + inf);
        }
        Thread.sleep(5000);
        inf = Info.request(client.getNodes()[0], "roster");
        System.out.println("NEW ROSTER:");
        System.out.println(inf);


		try {
			args.setServerSpecific(client);
		}
		catch (RuntimeException re) {
			client.close();
			throw re;
		}
	}

	@AfterClass
	public static void destroy() {
		System.out.println("End AerospikeClient");
		if (client != null) {
			client.close();
		}
	}
}
