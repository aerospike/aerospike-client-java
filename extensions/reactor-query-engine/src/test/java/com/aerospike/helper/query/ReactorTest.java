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
package com.aerospike.helper.query;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.reactor.AerospikeReactorClient;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import reactor.core.publisher.Mono;

import java.util.Arrays;

@RunWith(Parameterized.class)
abstract public class ReactorTest {

	final EventLoopType eventLoopType;

	EventLoops eventLoops;
	AerospikeClient client;
	AerospikeReactorClient reactorClient;

	ReactorQueryEngine queryEngine;

	@Parameterized.Parameters(name = "{0}")
	public static Iterable<Object[]> parameters() {
		return Arrays.asList(new Object[][] {
				{EventLoopType.DIRECT_NIO},
				{EventLoopType.NETTY_NIO}
		});
	}

	public ReactorTest(EventLoopType eventLoopType) {
		this.eventLoopType = eventLoopType;
	}

	@Before
	public void init(){

		switch (eventLoopType) {
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

			ClientPolicy clientPolicy = new ClientPolicy();
			clientPolicy.eventLoops = eventLoops;
			clientPolicy.timeout = TestQueryEngine.TIME_OUT;
			client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
			client.writePolicyDefault.expiration = 1800;
			client.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;

			this.reactorClient = new AerospikeReactorClient(client, eventLoops);

			this.queryEngine = new ReactorQueryEngine(reactorClient);
			queryEngine.refreshIndexes().block();

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
		queryEngine.close();
	}

	protected void tryDropIndex(String namespace, String setName, String indexName) {
		reactorClient.dropIndex(null, namespace, setName, indexName)
				.onErrorResume(throwable -> throwable instanceof AerospikeException
								&& ((AerospikeException) throwable).getResultCode() == ResultCode.INDEX_NOTFOUND,
						throwable -> Mono.empty())
				.then(queryEngine.refreshIndexes())
				.block();
	}

	protected void tryCreateIndex(String namespace, String setName, String indexName, String binName, IndexType indexType) {
		tryCreateIndex(namespace, setName, indexName, binName, indexType, IndexCollectionType.DEFAULT);
	}

	protected void tryCreateIndex(String namespace, String setName, String indexName, String binName, IndexType indexType,
								  IndexCollectionType collectionType) {
		reactorClient.createIndex(null, namespace, setName, indexName, binName, indexType, collectionType)
				.then(queryEngine.refreshIndexes())
				.block();
	}

}
