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
package com.aerospike.test.sync.basic;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.test.sync.TestSync;

public class TestAppend extends TestSync {
	@Test
	public void append() {
		Key key = new Key(args.namespace, args.set, "appendkey");
		String binName = args.getBinName("appendbin");

		// Delete record if it already exists.
		client.delete(null, key);

		Bin bin = new Bin(binName, "Hello");
		client.append(null, key, bin);

		bin = new Bin(binName, " World");
		client.append(null, key, bin);

		Record record = client.get(null, key, bin.name);
		assertBinEqual(key, record, bin.name, "Hello World");
	}

	@Test
	public void prepend() {
		Key key = new Key(args.namespace, args.set, "prependkey");
		String binName = args.getBinName("prependbin");

		// Delete record if it already exists.
		client.delete(null, key);

		Bin bin = new Bin(binName, "World");
		client.prepend(null, key, bin);

		bin = new Bin(binName, "Hello ");
		client.prepend(null, key, bin);

		Record record = client.get(null, key, bin.name);
		assertBinEqual(key, record, bin.name, "Hello World");
	}
}
