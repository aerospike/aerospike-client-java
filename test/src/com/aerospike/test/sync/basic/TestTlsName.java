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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.sync.TestSync;

import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;

/**
 * Server certificate name validation on the sync and netty paths, against the configured TLS
 * cluster.
 */
public class TestTlsName extends TestSync {
	private static final String WRONG_NAME = "wrong-tls-name.invalid";
	private static final String TLS_NAME_PREFIX = "Invalid TLS name: ";
	private static final int TIMEOUT_MS = 5000;
	private static final int ASYNC_WAIT_SECONDS = 10;

	private static Host host;

	@BeforeClass
	public static void requireTls() {
		Assume.assumeTrue("Requires -tls", args.tlsPolicy != null && !args.tlsPolicy.forLoginOnly);
		host = Host.parseHosts(args.host, args.port)[0];
		Assume.assumeTrue("Requires a tlsName in -h", host.tlsName != null);
	}

	@Test
	public void syncAcceptsConfiguredName() {
		connect(host.tlsName).close();
	}

	@Test
	public void syncRejectsWrongName() {
		try {
			connect(WRONG_NAME).close();
			fail("Connection with tlsName " + WRONG_NAME + " must be rejected");
		}
		catch (AerospikeException ae) {
			assertRejected(ae);
		}
	}

	@Test
	public void nettyAcceptsConfiguredName() throws Exception {
		NettyEventLoops eventLoops = nettyEventLoops();

		try (AerospikeClient nettyClient = new AerospikeClient(nettyPolicy(eventLoops), host)) {
			Key key = new Key(args.namespace, args.set, "tlsname-netty");
			put(nettyClient, eventLoops, key).get(ASYNC_WAIT_SECONDS, TimeUnit.SECONDS);

			CompletableFuture<Record> get = new CompletableFuture<>();

			nettyClient.get(eventLoops.next(), new RecordListener() {
				public void onSuccess(Key k, Record r) {
					get.complete(r);
				}
				public void onFailure(AerospikeException e) {
					get.completeExceptionally(e);
				}
			}, null, key);

			Record rec = get.get(ASYNC_WAIT_SECONDS, TimeUnit.SECONDS);
			assertBinEqual(key, rec, "v", "tlsname");
		}
		finally {
			eventLoops.close();
		}
	}

	@Test
	public void nettyRejectsWrongName() throws Exception {
		NettyEventLoops eventLoops = nettyEventLoops();

		try (AerospikeClient nettyClient = new AerospikeClient(nettyPolicy(eventLoops), host)) {
			// Tend validates the seed tlsName synchronously, so swap the node's tlsName after
			// connecting. The first async command then opens a netty connection with it.
			Node node = nettyClient.getNodes()[0];
			Host h = node.getHost();
			Field f = Node.class.getDeclaredField("host");
			f.setAccessible(true);
			f.set(node, new Host(h.name, WRONG_NAME, h.port));

			Key key = new Key(args.namespace, args.set, "tlsname-netty-reject");

			try {
				put(nettyClient, eventLoops, key).get(ASYNC_WAIT_SECONDS, TimeUnit.SECONDS);
				fail("Netty connection with tlsName " + WRONG_NAME + " must be rejected");
			}
			catch (ExecutionException ee) {
				assertTrue("Unexpected cause: " + ee.getCause(), ee.getCause() instanceof AerospikeException);
				assertRejected((AerospikeException)ee.getCause());
			}
		}
		finally {
			eventLoops.close();
		}
	}

	private static Connection connect(String tlsName) {
		return new Connection(args.tlsPolicy, tlsName, new InetSocketAddress(host.name, host.port), TIMEOUT_MS);
	}

	private static void assertRejected(AerospikeException ae) {
		assertFalse("Must not be retryable: " + ae, ae instanceof AerospikeException.Connection);
		assertEquals(TLS_NAME_PREFIX + WRONG_NAME, ae.getBaseMessage());
	}

	private static NettyEventLoops nettyEventLoops() {
		return new NettyEventLoops(new EventPolicy(),
			new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()), EventLoopType.NETTY_NIO);
	}

	private static ClientPolicy nettyPolicy(NettyEventLoops eventLoops) {
		ClientPolicy policy = new ClientPolicy();
		args.setClientPolicy(policy);
		policy.eventLoops = eventLoops;
		policy.timeout = TIMEOUT_MS;
		policy.failIfNotConnected = true;
		return policy;
	}

	private static CompletableFuture<Void> put(AerospikeClient nettyClient, NettyEventLoops eventLoops, Key key) {
		CompletableFuture<Void> put = new CompletableFuture<>();

		nettyClient.put(eventLoops.next(), new WriteListener() {
			public void onSuccess(Key k) {
				put.complete(null);
			}
			public void onFailure(AerospikeException e) {
				put.completeExceptionally(e);
			}
		}, null, key, new Bin("v", "tlsname"));
		return put;
	}
}
