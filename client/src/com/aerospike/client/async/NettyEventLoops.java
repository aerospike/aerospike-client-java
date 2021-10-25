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

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.KeyManagerFactory;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;

/**
 * Aerospike wrapper around netty event loops.
 * Implements the Aerospike EventLoops interface.
 */
public final class NettyEventLoops implements EventLoops, CipherSuiteFilter {

	private final Map<EventExecutor,NettyEventLoop> eventLoopMap;
	private final NettyEventLoop[] eventLoopArray;
	private final EventLoopGroup group;
	TlsPolicy tlsPolicy;
	SslContext sslContext;
	final EventLoopType eventLoopType;

	/**
	 * Create Aerospike event loop wrappers from given netty event loops.
	 */
	public NettyEventLoops(EventLoopGroup group) {
		this(new EventPolicy(), group);
	}

	/**
	 * Create Aerospike event loop wrappers from given netty event loops.
	 * The type of event loop is determined from the event loop group instance.
	 */
	public NettyEventLoops(EventPolicy policy, EventLoopGroup group) {
		this(policy, group, getEventLoopType(group));
	}

	/**
	 * Create Aerospike event loop wrappers from given netty event loops and specified event loop type.
	 */
	public NettyEventLoops(EventPolicy policy, EventLoopGroup group, EventLoopType type) {
		if (policy.minTimeout < 5) {
			throw new AerospikeException("Invalid minTimeout " + policy.minTimeout + ". Must be at least 5ms.");
		}
		this.group = group;
		this.eventLoopType = type;

		ArrayList<NettyEventLoop> list = new ArrayList<NettyEventLoop>();
		Iterator<EventExecutor> iter = group.iterator();
		int count = 0;

		while (iter.hasNext()) {
			EventExecutor eventExecutor = iter.next();
			list.add(new NettyEventLoop(policy, (io.netty.channel.EventLoop)eventExecutor, this, count++));
		}

		eventLoopArray = list.toArray(new NettyEventLoop[count]);
		eventLoopMap = new IdentityHashMap<EventExecutor,NettyEventLoop>(count);

		for (NettyEventLoop eventLoop : eventLoopArray) {
			eventLoopMap.put(eventLoop.eventLoop, eventLoop);
		}

		// Start timer in each event loop thread.
		for (NettyEventLoop eventLoop : eventLoopArray) {
			eventLoop.execute(new Runnable() {
				public void run() {
					eventLoop.timer.start();
				}
			});
		}
	}

	private static EventLoopType getEventLoopType(EventLoopGroup group) {
		// Wrap each instanceof comparison in a try/catch block because these classes reference
		// libraries that are optional and might not be specified in the build file (pom.xml).
		// This is preferable to using a "getClass().getSimpleName()" switch statement because
		// that requires exact classname equality and does not handle custom classes that might
		// inherit from these classes.
		try {
			if (group instanceof NioEventLoopGroup) {
				return EventLoopType.NETTY_NIO;
			}
		}
		catch (NoClassDefFoundError e) {
		}

		try {
			if (group instanceof EpollEventLoopGroup) {
				return EventLoopType.NETTY_EPOLL;
			}
		}
		catch (NoClassDefFoundError e) {
		}

		try {
			if (group instanceof KQueueEventLoopGroup) {
				return EventLoopType.NETTY_KQUEUE;
			}
		}
		catch (NoClassDefFoundError e) {
		}

		try {
			if (group instanceof IOUringEventLoopGroup) {
				return EventLoopType.NETTY_IOURING;
			}
		}
		catch (NoClassDefFoundError e) {
		}

		throw new AerospikeException("Unexpected EventLoopGroup");
	}

	/**
	 * Initialize event loops with client policy. For internal use only.
	 */
	@Override
	public void init(ClientPolicy policy) {
		if (policy.tlsPolicy != null) {
			initTlsContext(policy.tlsPolicy);
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

		if (policy.context != null) {
			CipherSuiteFilter csf = (policy.ciphers != null)? this : IdentityCipherSuiteFilter.INSTANCE;
			sslContext = new JdkSslContext(policy.context, true, null, csf, null, ClientAuth.NONE, null, false);
			return;
		}

		try {
			SslContextBuilder builder = SslContextBuilder.forClient();

			if (policy.protocols != null) {
				builder.protocols(policy.protocols);
			}

			if (policy.ciphers != null) {
				builder.ciphers(Arrays.asList(policy.ciphers));
			}

			String keyStoreLocation = System.getProperty("javax.net.ssl.keyStore");

			// Keystore is only required for mutual authentication.
			if (keyStoreLocation != null) {
				String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
				char[] pass = (keyStorePassword != null) ? keyStorePassword.toCharArray() : null;

				KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
				FileInputStream is = new FileInputStream(keyStoreLocation);

				try {
					ks.load(is, pass);
				}
				finally {
					is.close();
				}

				KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
				kmf.init(ks, pass);

				builder.keyManager(kmf);
			}

			sslContext = builder.build();
		}
		catch (Exception e) {
			throw new AerospikeException("Failed to init netty TLS: " + Util.getErrorMessage(e));
		}
	}

	/**
	 * Filter cipher suites.  For internal use only.
	 */
	@Override
	public String[] filterCipherSuites(Iterable<String> ciphers, List<String> defaultCiphers, Set<String> supportedCiphers) {
		if (tlsPolicy.ciphers != null) {
			return tlsPolicy.ciphers;
		}
		return tlsPolicy.context.getSupportedSSLParameters().getCipherSuites();
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
	 * Return Aerospike event loop given array index.
	 */
	@Override
	public NettyEventLoop get(int index) {
		return eventLoopArray[index];
	}

	/**
	 * Return next Aerospike event loop as determined by {@link io.netty.channel.EventLoopGroup#next()}.
	 */
	@Override
	public NettyEventLoop next() {
		return eventLoopMap.get(group.next());
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
