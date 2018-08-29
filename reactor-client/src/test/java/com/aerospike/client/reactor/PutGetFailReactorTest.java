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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.ReactorTest;
import com.aerospike.client.reactor.util.Args;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.client.reactor.util.ReactorUtil.succeedAfterRetries;

public class PutGetFailReactorTest extends ReactorTest {

	public PutGetFailReactorTest(Args args) {
		super(args);
	}

	@Before
	public void setDelayOnProxy(){
		proxy.setDelayOnResponse(100);
	}

	@Test
	public void shouldFailOnGet() {
		final Key key = new Key(args.namespace, args.set, "putgetkey1");

		Mono<KeyRecord> mono = reactorClient.get(strictReadPolicy(), key);

		StepVerifier.create(mono)
				.expectError(AerospikeException.Timeout.class);
	}

	private Policy strictReadPolicy() {
		Policy strictPolicy = new Policy();
		strictPolicy.setTimeouts(1, 1);
		return strictPolicy;
	}

}
