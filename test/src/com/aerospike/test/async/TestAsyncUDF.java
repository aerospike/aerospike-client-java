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
package com.aerospike.test.async;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.task.RegisterTask;

public class TestAsyncUDF extends TestAsync {
	private static final String binName = args.getBinName("audfbin1");

	@BeforeClass
	public static void prepare() {
		RegisterTask rtask = client.register(null, TestAsyncUDF.class.getClassLoader(), "udf/record_example.lua", "record_example.lua", Language.LUA);
		rtask.waitTillComplete();
	}

	@Test
	public void asyncUDF() {
		final Key key = new Key(args.namespace, args.set, "audfkey1");
		final Bin bin = new Bin(binName, "string value");

		client.execute(eventLoop, new ExecuteListener() {

			public void onSuccess(final Key key, final Object obj) {
				try {
					// Write succeeded.  Now call read using udf.
					client.execute(eventLoop, new ExecuteListener() {

						public void onSuccess(final Key key, final Object received) {
							Object expected = bin.value.getObject();

							if (assertNotNull(received)) {
								assertEquals(expected, received);
							}
							notifyComplete();
						}

						public void onFailure(AerospikeException e) {
							setError(e);
							notifyComplete();
						}

					}, null, key, "record_example", "readBin", Value.get(bin.name));
				}
				catch (Exception e) {
					setError(e);
					notifyComplete();
				}
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}

		}, null, key, "record_example", "writeBin", Value.get(bin.name), bin.value);

		waitTillComplete();
	}
}
