/*
 * Copyright 2012-2018 Aerospike, Inc.
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
package com.aerospike.client.reactor;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.*;
import com.aerospike.client.reactor.util.Args;
import org.junit.After;
import org.junit.Before;
import org.netcrusher.core.reactor.NioReactor;
import org.netcrusher.tcp.TcpCrusher;
import org.netcrusher.tcp.TcpCrusherBuilder;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract public class ReactorFailTest extends ReactorTest{

	List<TcpCrusher> proxies;
	AerospikeReactorClient proxyReactorClient;

	public ReactorFailTest(Args args) {
		super(args);
	}

	@Before
	public void initProxy(){

		ClientPolicy policy = new ClientPolicy();
		policy.eventLoops = eventLoops;
		policy.user = args.user;
		policy.password = args.password;
		policy.authMode = args.authMode;
		policy.tlsPolicy = args.tlsPolicy;
		policy.timeout = 100;

		AerospikeClient discoveryClient = new AerospikeClient(policy, new Host(args.host, args.port));

		initProxiesForAllNodes(discoveryClient);

		try {

			Host[] hosts = proxies.stream()
					.map(proxy -> new Host(proxy.getBindAddress().getHostName(), proxy.getBindAddress().getPort()))
					.toArray(Host[]::new);

			AerospikeClient proxyClient = new AerospikeClient(policy, hosts);
			this.proxyReactorClient = new AerospikeReactorClient(proxyClient, eventLoops);

		}
		catch (Throwable e) {
			try {
				proxies.forEach(TcpCrusher::close);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			throw new RuntimeException(e);
		}

		proxies.forEach(TcpCrusher::freeze);
	}

	private void initProxiesForAllNodes(AerospikeClient discoveryClient) {
		try {
			NioReactor reactor = new NioReactor();
			proxies = Stream.of(discoveryClient.getNodes())
					.map(Node::getHost)
					.map(host -> {
						try {
							return TcpCrusherBuilder.builder()
									.withReactor(reactor)
									.withBindAddress("localhost", getFreePort())
									.withConnectAddress(host.name, host.port)
									.buildAndOpen();
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}).collect(Collectors.toList());
		} catch (IOException e){
			eventLoops.close();
			throw new RuntimeException(e);
		}
	}

	private int getFreePort() throws IOException {
		try (ServerSocket serverSocket = new ServerSocket(0)) {
			return serverSocket.getLocalPort();
		}
	}

	@After
	public void destroyProxy()  {
		proxies.forEach(proxy -> {
			proxy.unfreeze();
			proxy.close();
		});
	}

	Policy strictReadPolicy() {
		Policy strictPolicy = new Policy();
		strictPolicy.setTimeouts(1, 1);
		return strictPolicy;
	}

	WritePolicy strictWritePolicy() {
		WritePolicy strictPolicy = new WritePolicy();
		strictPolicy.setTimeouts(1, 1);
		return strictPolicy;
	}

	QueryPolicy strictQueryPolicy() {
		QueryPolicy strictPolicy = new QueryPolicy();
		strictPolicy.setTimeouts(1, 1);
		return strictPolicy;
	}

	ScanPolicy strictScanPolicy() {
		ScanPolicy strictPolicy = new ScanPolicy();
		strictPolicy.setTimeouts(1, 1);
		return strictPolicy;
	}

	BatchPolicy strictBatchPolicy() {
		BatchPolicy strictPolicy = new BatchPolicy();
		strictPolicy.setTimeouts(1, 1);
		return strictPolicy;
	}

}
