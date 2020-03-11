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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.function.Predicate;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.util.Args;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

@RunWith(Parameterized.class)
abstract public class ReactorTest {

	protected final Args args;

	EventLoops eventLoops;
	protected AerospikeReactorClient reactorClient;
	protected AerospikeClient client;

	@Parameterized.Parameters(name = "{0}")
	public static Iterable<Object[]> parameters() {
		return Arrays.asList(new Object[][] {
				{new Args().setEventLoopType(EventLoopType.DIRECT_NIO)},
				{new Args().setEventLoopType(EventLoopType.NETTY_NIO)}
		});
	}

	public ReactorTest(Args args) {
		this.args = args;
	}

	@Before
	public void init(){

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
			this.client = new AerospikeClient(policy, hosts);
			this.reactorClient = new AerospikeReactorClient(client, eventLoops);

			try {
				args.setServerSpecific(client);
			}
			catch (Throwable re) {
				client.close();
				throw re;
			}
		}
		catch (Throwable e) {
			eventLoops.close();
			throw new RuntimeException(e);
		}
	}

	@After
	public void destroy() throws Exception {
		reactorClient.close();
		eventLoops.close();
	}

	protected Predicate<KeyRecord> checkKeyRecord(Key key, String binName, Object binValue) {
		return keyRecord -> keyRecord.key.equals(key) && keyRecord.record.bins.get(binName).equals(binValue);
	}

	public void assertBinEqual(Key key, Record record, Bin bin) {
		assertRecordFound(key, record);

		Object received = record.getValue(bin.name);
		Object expected = bin.value.getObject();

		assertThat(received).isEqualTo(expected);
	}

	public void assertBinEqual(Key key, Record record, String binName, Object expected) {
		assertRecordFound(key, record);

		Object received = record.getValue(binName);
		assertThat(received).isEqualTo(expected);
	}

	public void assertRecordFound(Key key, Record record) {
		if (record == null) {
			throw new IllegalArgumentException("Failed to get: namespace=" + args.namespace + " set=" + args.set + " key=" + key.userKey);
		}
 	}

}
