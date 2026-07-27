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
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Version;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.kqueue.KQueueIoHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.uring.IoUringIoHandler;

public final class ExampleRunner {
	private record ServerFacts(
		Version serverVersion,
		boolean enterpriseEdition,
		boolean strongConsistencyNamespace,
		boolean ttlSupported
	) {
	}

	private final Console console;
	private final Parameters params;

	public ExampleRunner(Console console, Parameters params) {
		this.console = console;
		this.params = params;
	}

	public ExampleRunResult runSync(List<ExampleDefinition> definitions) {
		List<ExampleResult> results = new ArrayList<>();
		try {
			ClientPolicy policy = createClientPolicy(null);
			try (IAerospikeClient client = createClient(policy)) {
				ServerFacts serverFacts = loadServerFacts(client);

				for (ExampleDefinition definition : definitions) {
					results.add(runOneSync(definition, client, serverFacts));
				}
			}
		}
		catch (Throwable failure) {
			logBootstrapFailure("Sync", failure);
			return bootstrapFailure(definitions, "Sync", failure);
		}
		return new ExampleRunResult(results);
	}

	public ExampleRunResult runAsync(List<ExampleDefinition> definitions) {
		List<ExampleResult> results = new ArrayList<>();
		try (EventLoops eventLoops = createEventLoops()) {
			ClientPolicy policy = createClientPolicy(eventLoops);
			try (IAerospikeClient client = createClient(policy)) {
				ServerFacts serverFacts = loadServerFacts(client);
				EventLoop eventLoop = eventLoops.get(0);

				for (ExampleDefinition definition : definitions) {
					results.add(runOneAsync(definition, client, eventLoop, serverFacts));
				}
			}
		}
		catch (Throwable failure) {
			logBootstrapFailure("Async", failure);
			return bootstrapFailure(definitions, "Async", failure);
		}
		return new ExampleRunResult(results);
	}

	private ExampleResult runOneSync(
		ExampleDefinition definition,
		IAerospikeClient client,
		ServerFacts serverFacts
	) {
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
		boolean cleanupNeeded = false;

		console.info(definition.name() + " Begin");
		try {
			enforceServerRequirement(definition, serverFacts);
			Object instance = definition.exampleClass().getDeclaredConstructor().newInstance();

			if (! (instance instanceof Example example)) {
				throw new IllegalArgumentException("Invalid sync example class: " + definition.exampleClass().getName());
			}
			example.initialize(client, params, console);
			cleanupNeeded = true;
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

		Throwable cleanupFailure = cleanupNeeded ? cleanup(definition, client) : null;
		console.info(definition.name() + " End");
		return finishResult(definition.name(), startNanos, errorCountBefore, failure, skipMessage, cleanupFailure);
	}

	private ExampleResult runOneAsync(
		ExampleDefinition definition,
		IAerospikeClient client,
		EventLoop eventLoop,
		ServerFacts serverFacts
	) {
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
		boolean cleanupNeeded = false;

		console.info(definition.name() + " Begin");
		try {
			enforceServerRequirement(definition, serverFacts);
			Object instance = definition.exampleClass().getDeclaredConstructor().newInstance();

			if (! (instance instanceof AsyncExample example)) {
				throw new IllegalArgumentException("Invalid async example class: " + definition.exampleClass().getName());
			}
			example.initialize(client, eventLoop, params, console);
			cleanupNeeded = true;
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

		Throwable cleanupFailure = cleanupNeeded ? cleanup(definition, client) : null;
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

		default:
			throw new IllegalArgumentException("Unsupported event loop type: " + params.eventLoopType);
		}
	}

	private ServerFacts loadServerFacts(IAerospikeClient client) {
		Node[] nodes = client.getNodes();

		if (nodes == null || nodes.length == 0) {
			throw new IllegalStateException("Connected client did not return any cluster nodes");
		}

		Node node = nodes[0];
		Version infoNodeVersion = node.getServerVersion();
		Version serverVersion = minimumServerVersion(nodes);
		String editionFilter = infoNodeVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "release" : "edition";
		String namespaceFilter = "namespace/" + params.namespace();
		Map<String,String> info = Info.request(null, node, editionFilter, namespaceFilter);
		String editionToken = info.get(editionFilter);

		if (editionToken == null) {
			throw new IllegalStateException("Failed to get server edition for example gating");
		}

		String namespaceTokens = info.get(namespaceFilter);

		if (namespaceTokens == null) {
			throw new IllegalStateException("Failed to get namespace info for " + params.namespace());
		}

		Partitions partitions = client.getCluster().partitionMap.get(params.namespace());
		boolean strongConsistencyNamespace = partitions != null && partitions.scMode;
		int nsup = Integer.parseInt(parseString(namespaceTokens, "nsup-period"));
		boolean ttlSupported = nsup != 0 || Boolean.parseBoolean(parseString(namespaceTokens, "allow-ttl-without-nsup"));
		boolean enterpriseEdition = editionToken.equals("Aerospike Enterprise Edition") || editionToken.contains("Enterprise");
		return new ServerFacts(serverVersion, enterpriseEdition, strongConsistencyNamespace, ttlSupported);
	}

	private Version minimumServerVersion(Node[] nodes) {
		Version minimum = nodes[0].getServerVersion();

		for (int i = 1; i < nodes.length; i++) {
			Version candidate = nodes[i].getServerVersion();

			if (! candidate.isGreaterOrEqual(minimum)) {
				minimum = candidate;
			}
		}
		return minimum;
	}

	private void enforceServerRequirement(ExampleDefinition definition, ServerFacts serverFacts)
		throws ExampleSkipException {
		String unmetReason = definition.serverRequirement().unmetReason(
			serverFacts.serverVersion(),
			serverFacts.enterpriseEdition(),
			serverFacts.strongConsistencyNamespace(),
			serverFacts.ttlSupported());

		if (unmetReason != null) {
			throw new ExampleSkipException(unmetReason);
		}
	}

	private static String parseString(String namespaceTokens, String name) {
		String search = name + '=';
		int begin = namespaceTokens.indexOf(search);

		if (begin < 0) {
			throw new IllegalStateException("Failed to find namespace config token: " + name);
		}

		begin += search.length();
		int end = namespaceTokens.indexOf(';', begin);

		if (end < 0) {
			end = namespaceTokens.length();
		}
		return namespaceTokens.substring(begin, end);
	}

	private void logFailure(String name, Throwable failure) {
		console.error("%s failed%n%s", name, stackTraceOf(failure));
	}

	private void logBootstrapFailure(String mode, Throwable failure) {
		console.error("%s runner bootstrap failed%n%s", mode, stackTraceOf(failure));
	}

	private ExampleRunResult bootstrapFailure(
		List<ExampleDefinition> definitions,
		String mode,
		Throwable failure
	) {
		List<ExampleResult> results = new ArrayList<>(definitions.size());

		for (ExampleDefinition definition : definitions) {
			IllegalStateException wrapped = new IllegalStateException(
				mode + " runner bootstrap failed before executing " + definition.name(),
				failure);
			results.add(ExampleResult.failed(definition.name(), 0, wrapped));
		}
		return new ExampleRunResult(results);
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
