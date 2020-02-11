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
package com.aerospike.examples;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.lang.reflect.Constructor;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public abstract class AsyncExample {
	/**
	 * Connect and run one or more asynchronous client examples.
	 */
	public static void runExamples(Console console, Parameters params, List<String> examples) throws Exception {
		EventPolicy eventPolicy = new EventPolicy();
		eventPolicy.maxCommandsInProcess = params.maxCommandsInProcess;
		eventPolicy.maxCommandsInQueue = params.maxCommandsInQueue;

		EventLoops eventLoops;

		switch (params.eventLoopType) {
			default:
			case DIRECT_NIO: {
				eventLoops = new NioEventLoops(eventPolicy, 1);
				break;
			}

			case NETTY_NIO: {
				EventLoopGroup group = new NioEventLoopGroup(1);
				eventLoops = new NettyEventLoops(eventPolicy, group);
				break;
			}

			case NETTY_EPOLL: {
				EventLoopGroup group = new EpollEventLoopGroup(1);
				eventLoops = new NettyEventLoops(eventPolicy, group);
				break;
			}
		}

		try {
			ClientPolicy policy = new ClientPolicy();
			policy.eventLoops = eventLoops;
			policy.user = params.user;
			policy.password = params.password;
			policy.authMode = params.authMode;
			policy.tlsPolicy = params.tlsPolicy;

			params.policy = policy.readPolicyDefault;
			params.writePolicy = policy.writePolicyDefault;

			Host[] hosts = Host.parseHosts(params.host, params.port);

			AerospikeClient client = new AerospikeClient(policy, hosts);

			try {
				EventLoop eventLoop = eventLoops.get(0);
				params.setServerSpecific(client);

				for (String exampleName : examples) {
					runExample(exampleName, client, eventLoop, params, console);
				}
			}
			finally {
				client.close();
			}
		}
		finally {
			eventLoops.close();
		}
	}

	/**
	 * Run asynchronous client example.
	 */
	public static void runExample(String exampleName, AerospikeClient client, EventLoop eventLoop, Parameters params, Console console) throws Exception {
		String fullName = "com.aerospike.examples." + exampleName;
		Class<?> cls = Class.forName(fullName);

		if (AsyncExample.class.isAssignableFrom(cls)) {
			Constructor<?> ctor = cls.getConstructor();
			AsyncExample example = (AsyncExample)ctor.newInstance();
			example.console = console;
			example.params = params;
			example.writePolicy = client.writePolicyDefault;
			example.policy = client.readPolicyDefault;
			example.run(client, eventLoop);
		}
		else {
			console.error("Invalid example: " + exampleName);
		}
	}

	protected Console console;
	protected Parameters params;
	protected WritePolicy writePolicy;
	protected Policy policy;
	private boolean completed;

	public void run(AerospikeClient client, EventLoop eventLoop) {
		// Most async examples no longer wait for completion, so
		// these examples are run in parallel with intertwined log
		// messages.  It's done that way because most applications
		// should be written this way for performance reasons.
		//
		// Fortunately, AerospikeClient.close() now waits for pending
		// async commands to complete before closing.
		console.info("Example: " + this.getClass().getSimpleName());
		runExample(client, eventLoop);
	}

	protected void resetComplete() {
		completed = false;
	}

	protected synchronized void waitTillComplete() {
		while (! completed) {
			try {
				super.wait();
			}
			catch (InterruptedException ie) {
			}
		}
	}

	protected synchronized void notifyComplete() {
		completed = true;
		super.notify();
	}

	public abstract void runExample(AerospikeClient client, EventLoop eventLoop);
}
