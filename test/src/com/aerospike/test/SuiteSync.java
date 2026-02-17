/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.sync.basic.TestAdd;
import com.aerospike.test.sync.basic.TestAppend;
import com.aerospike.test.sync.basic.TestBatch;
import com.aerospike.test.sync.basic.TestBitExp;
import com.aerospike.test.sync.basic.TestConfigLoadYAML;
import com.aerospike.test.sync.basic.TestDeleteBin;
import com.aerospike.test.sync.basic.TestExpOperation;
import com.aerospike.test.sync.basic.TestExpire;
import com.aerospike.test.sync.basic.TestFilterExp;
import com.aerospike.test.sync.basic.TestGeneration;
import com.aerospike.test.sync.basic.TestHLLExp;
import com.aerospike.test.sync.basic.TestListExp;
import com.aerospike.test.sync.basic.TestListMap;
import com.aerospike.test.sync.basic.TestMapExp;
import com.aerospike.test.sync.basic.TestOperate;
import com.aerospike.test.sync.basic.TestOperateBit;
import com.aerospike.test.sync.basic.TestOperateHll;
import com.aerospike.test.sync.basic.TestOperateList;
import com.aerospike.test.sync.basic.TestOperateMap;
import com.aerospike.test.sync.basic.TestPutGet;
import com.aerospike.test.sync.basic.TestQueryRoles;
import com.aerospike.test.sync.basic.TestReplace;
import com.aerospike.test.sync.basic.TestScan;
import com.aerospike.test.sync.basic.TestServerInfo;
import com.aerospike.test.sync.basic.TestTouch;
import com.aerospike.test.sync.basic.TestTxn;
import com.aerospike.test.sync.basic.TestUDF;
import com.aerospike.test.sync.query.TestIndex;
import com.aerospike.test.sync.query.TestExpSecondaryIndex;
import com.aerospike.test.sync.query.TestQueryAverage;
import com.aerospike.test.sync.query.TestQueryBlob;
import com.aerospike.test.sync.query.TestQueryCollection;
import com.aerospike.test.sync.query.TestQueryContext;
import com.aerospike.test.sync.query.TestQueryExecute;
import com.aerospike.test.sync.query.TestQueryFilter;
import com.aerospike.test.sync.query.TestQueryFilterExp;
import com.aerospike.test.sync.query.TestQueryFilterSet;
import com.aerospike.test.sync.query.TestQueryGeo;
import com.aerospike.test.sync.query.TestQueryInteger;
import com.aerospike.test.sync.query.TestQueryKey;
import com.aerospike.test.sync.query.TestQueryRPS;
import com.aerospike.test.sync.query.TestQueryString;
import com.aerospike.test.sync.query.TestQuerySum;
import com.aerospike.test.util.Args;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestAdd.class,
	TestAppend.class,
	TestBatch.class,
	TestBitExp.class,
	TestConfigLoadYAML.class,
	TestDeleteBin.class,
	TestExpire.class,
	TestExpOperation.class,
	TestFilterExp.class,
	TestGeneration.class,
	TestHLLExp.class,
	TestListExp.class,
	TestListMap.class,
	TestMapExp.class,
	TestOperate.class,
	TestOperateBit.class,
	TestOperateHll.class,
	TestOperateList.class,
	TestOperateMap.class,
	TestPutGet.class,
	TestQueryRoles.class,
	TestReplace.class,
	TestScan.class,
	TestServerInfo.class,
	TestTouch.class,
	TestTxn.class,
	TestUDF.class,
	TestIndex.class,
	TestExpSecondaryIndex.class,
	TestQueryAverage.class,
	TestQueryBlob.class,
	TestQueryCollection.class,
	TestQueryContext.class,
	TestQueryExecute.class,
	TestQueryFilter.class,
	TestQueryFilterExp.class,
	TestQueryFilterSet.class,
	TestQueryGeo.class,
	TestQueryInteger.class,
	TestQueryKey.class,
	TestQueryRPS.class,
	TestQueryString.class,
	TestQuerySum.class
})
public class SuiteSync {
	public static IAerospikeClient client = null;

	@BeforeClass
	public static void init() {
		Log.setCallback(null);

		System.out.println("Begin AerospikeClient");
		Args args = Args.Instance;

		ClientPolicy policy = new ClientPolicy();
		args.setClientPolicy(policy);

		Host[] hosts = Host.parseHosts(args.host, args.port);

		client = new AerospikeClient(policy, hosts);

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
		System.out.println("End AerospikeClient");
		if (client != null) {
			client.close();
		}
	}
}
