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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.dto.KeyObject;
import com.aerospike.client.reactor.util.Args;
import com.aerospike.client.task.RegisterTask;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class UdfReactorTest extends ReactorTest {

	private final String binName = args.getBinName("audfbin1");

	public UdfReactorTest(Args args) {
		super(args);
	}

	@Before
	public void registerUdf() {
		RegisterTask rtask = client.register(null, UdfReactorTest.class.getClassLoader(), "udf/record_example.lua", "record_example.lua", Language.LUA);
		rtask.waitTillComplete();
	}

	@Test
	public void udf() {
		final Key key = new Key(args.namespace, args.set, "audfkey1");
		final Bin bin = new Bin(binName, "string value");		
		
		Mono<KeyObject> mono = reactorClient.execute(key,
				"record_example", "writeBin", Value.get(bin.name), bin.value)
				// Write succeeded.  Now call read using udf.
				.flatMap(keyObject -> reactorClient.execute(key,
					"record_example", "readBin", Value.get(bin.name)));

		StepVerifier.create(mono)
				.expectNextMatches(keyObject -> {
					Object expected = bin.value.getObject();
					assertNotNull(keyObject.value);
					assertEquals(expected, keyObject.value);
					return true;
				})
				.verifyComplete();
	}
}
