/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.aerospike.client.Host;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.test.async.TestAsyncBatch;
import com.aerospike.test.async.TestAsyncOperate;
import com.aerospike.test.async.TestAsyncPutGet;
import com.aerospike.test.async.TestAsyncQuery;
import com.aerospike.test.async.TestAsyncScan;
import com.aerospike.test.async.TestAsyncUDF;
import com.aerospike.test.util.Args;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestAsyncPutGet.class,
	TestAsyncBatch.class,
	TestAsyncOperate.class,
	TestAsyncScan.class,
	TestAsyncQuery.class,
	TestAsyncUDF.class
})
public class SuiteAsync {
	public static AsyncClient client = null;

	@BeforeClass
	public static void init() {
		System.out.println("Begin AsyncClient");
		Args args = Args.Instance;
		
		AsyncClientPolicy policy = new AsyncClientPolicy();
		policy.user = args.user;
		policy.password = args.password;
		policy.asyncMaxCommands = 300;
		policy.asyncSelectorThreads = 1;
		policy.asyncSelectorTimeout = 10;
		policy.failIfNotConnected = true;
		
		Host[] hosts = Host.parseHosts(args.host, args.port);

		client = new AsyncClient(policy, hosts);
		
		try {
			args.setServerSpecific(client);
		}
		catch (RuntimeException re) {
			client.close();
			throw re;
		}
	}
	
	@AfterClass
	public static void destroy() {
		System.out.println("End AsyncClient");
		if (client != null) {
			client.close();
		}
	}
}
