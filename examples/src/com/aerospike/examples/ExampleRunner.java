/*
 * Copyright 2012-2026 Aerospike, Inc.
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.kqueue.KQueueIoHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.uring.IoUringIoHandler;

public final class ExampleRunner {
	private final Console console;
	private final Parameters params;

	public ExampleRunner(Console console, Parameters params) {
		this.console = console;
		this.params = params;
	}

	public ExampleRunResult runSync(List<ExampleDefinition> definitions) throws Exception {
		List<ExampleResult> results = new ArrayList<ExampleResult>();
		ClientPolicy policy = createClientPolicy(null);
		IAerospikeClient client = createClient(policy);

		try {
			for (ExampleDefinition definition : definitions) {
				results.add(runOneSync(definition, client));
			}
		}
		finally {
			client.close();
		}
		return new ExampleRunResult(results);
	}

	public ExampleRunResult runAsync(List<ExampleDefinition> definitions) throws Exception {
		List<ExampleResult> results = new ArrayList<ExampleResult>();
		EventLoops eventLoops = createEventLoops();

		try {
			ClientPolicy policy = createClientPolicy(eventLoops);
			IAerospikeClient client = createClient(policy);

			try {
				EventLoop eventLoop = eventLoops.get(0);

				for (ExampleDefinition definition : definitions) {
					results.add(runOneAsync(definition, client, eventLoop));
				}
			}
			finally {
				client.close();
			}
		}
		finally {
			eventLoops.close();
		}
		return new ExampleRunResult(results);
	}

	private ExampleResult runOneSync(ExampleDefinition definition, IAerospikeClient client) {
		if (definition.mode() != ExampleMode.SYNC) {
			return ExampleResult.failed(
				definition.name(),
				0,
				new IllegalArgumentException("Sync runner cannot execute mode " + definition.mode()));
		}

		int errorCountBefore = console.errorCount();
		long startNanos = System.nanoTime();
		Throwable failure = null;
		String skipMessage = null;

		try {
			Object instance = definition.exampleClass().getDeclaredConstructor().newInstance();

			if (! (instance instanceof Example)) {
				throw new IllegalArgumentException("Invalid sync example class: " + definition.exampleClass().getName());
			}

			Example example = (Example)instance;
			console.info(definition.name() + " Begin");
			example.initialize(client, params, console);
			definition.fixture().setup(client, params);
			example.runExample();
			definition.fixture().verify(client, params);
		}
		catch (ExampleSkipException skip) {
			console.info("%s skipped: %s", definition.name(), skip.getMessage());
			skipMessage = skip.getMessage();
		}
		catch (Throwable thrown) {
			logFailure(definition.name(), thrown);
			failure = thrown;
		}

		Throwable cleanupFailure = cleanup(definition, client);
		console.info(definition.name() + " End");
		return finishResult(definition.name(), startNanos, errorCountBefore, failure, skipMessage, cleanupFailure);
	}

	private ExampleResult runOneAsync(ExampleDefinition definition, IAerospikeClient client, EventLoop eventLoop) {
		if (definition.mode() != ExampleMode.ASYNC) {
			return ExampleResult.failed(
				definition.name(),
				0,
				new IllegalArgumentException("Async runner cannot execute mode " + definition.mode()));
		}

		int errorCountBefore = console.errorCount();
		long startNanos = System.nanoTime();
		Throwable failure = null;
		String skipMessage = null;

		try {
			Object instance = definition.exampleClass().getDeclaredConstructor().newInstance();

			if (! (instance instanceof AsyncExample)) {
				throw new IllegalArgumentException("Invalid async example class: " + definition.exampleClass().getName());
			}

			AsyncExample example = (AsyncExample)instance;
			console.info(definition.name() + " Begin");
			example.initialize(client, eventLoop, params, console);
			definition.fixture().setup(client, params);
			example.runExample();
			example.awaitCompletion();
			definition.fixture().verify(client, params);
		}
		catch (ExampleSkipException skip) {
			console.info("%s skipped: %s", definition.name(), skip.getMessage());
			skipMessage = skip.getMessage();
		}
		catch (Throwable thrown) {
			logFailure(definition.name(), thrown);
			failure = thrown;
		}

		Throwable cleanupFailure = cleanup(definition, client);
		console.info(definition.name() + " End");
		return finishResult(definition.name(), startNanos, errorCountBefore, failure, skipMessage, cleanupFailure);
	}

	private ClientPolicy createClientPolicy(EventLoops eventLoops) {
		ClientPolicy policy = new ClientPolicy();

		if (eventLoops != null) {
			policy.eventLoops = eventLoops;
		}
		policy.user = params.user;
		policy.password = params.password;
		policy.authMode = params.authMode;
		policy.tlsPolicy = params.tlsPolicy;

		params.policy = policy.readPolicyDefault;
		params.writePolicy = policy.writePolicyDefault;
		return policy;
	}

	private IAerospikeClient createClient(ClientPolicy policy) {
		Host[] hosts = Host.parseHosts(params.host, params.port);
		return new AerospikeClient(policy, hosts);
	}

	private EventLoops createEventLoops() {
		EventPolicy eventPolicy = new EventPolicy();
		eventPolicy.maxCommandsInProcess = params.maxCommandsInProcess;
		eventPolicy.maxCommandsInQueue = params.maxCommandsInQueue;

		switch (params.eventLoopType) {
		default:
		case DIRECT_NIO:
			return new NioEventLoops(eventPolicy, 1);

		case NETTY_NIO: {
			EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
			return new NettyEventLoops(eventPolicy, group, params.eventLoopType);
		}

		case NETTY_EPOLL: {
			EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
			return new NettyEventLoops(eventPolicy, group, params.eventLoopType);
		}

		case NETTY_KQUEUE: {
			EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, KQueueIoHandler.newFactory());
			return new NettyEventLoops(eventPolicy, group, params.eventLoopType);
		}

		case NETTY_IOURING: {
			EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
			return new NettyEventLoops(eventPolicy, group, params.eventLoopType);
		}
		}
	}

	private void logFailure(String name, Throwable failure) {
		console.error("%s failed%n%s", name, stackTraceOf(failure));
	}

	private Throwable cleanup(ExampleDefinition definition, IAerospikeClient client) {
		try {
			definition.fixture().cleanup(client, params);
			return null;
		}
		catch (Throwable cleanup) {
			console.error("%s cleanup failed: %s", definition.name(), cleanup.getMessage());
			return cleanup;
		}
	}

	private ExampleResult finishResult(
		String name,
		long startNanos,
		int errorCountBefore,
		Throwable failure,
		String skipMessage,
		Throwable cleanupFailure
	) {
		long elapsedMillis = elapsedMillis(startNanos);

		if (failure != null) {
			return ExampleResult.failed(name, elapsedMillis, failure);
		}

		if (skipMessage != null) {
			return ExampleResult.skipped(name, elapsedMillis, skipMessage);
		}

		if (cleanupFailure != null) {
			return ExampleResult.failed(name, elapsedMillis, cleanupFailure);
		}

		if (console.errorCount() > errorCountBefore) {
			return ExampleResult.failed(
				name,
				elapsedMillis,
				new IllegalStateException("example logged one or more errors"));
		}

		return ExampleResult.passed(name, elapsedMillis);
	}

	private long elapsedMillis(long startNanos) {
		return (System.nanoTime() - startNanos) / 1000000L;
	}

	private String stackTraceOf(Throwable failure) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		failure.printStackTrace(pw);
		pw.flush();
		return sw.toString();
	}
}
