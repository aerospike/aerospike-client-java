/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import com.aerospike.test.async.TestAsyncBatch;
import com.aerospike.test.async.TestAsyncOperate;
import com.aerospike.test.async.TestAsyncPutGet;
import com.aerospike.test.async.TestAsyncScan;
import com.aerospike.test.sync.basic.TestAdd;
import com.aerospike.test.sync.basic.TestAppend;
import com.aerospike.test.sync.basic.TestBatch;
import com.aerospike.test.sync.basic.TestBitExp;
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
import com.aerospike.test.sync.basic.TestReplace;
import com.aerospike.test.sync.basic.TestScan;
import com.aerospike.test.sync.basic.TestTouch;
import com.aerospike.test.util.Args;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	// TODO: add test for username and password with proxy client
	TestAsyncPutGet.class,
	TestAsyncBatch.class,
	TestAsyncOperate.class,
	TestAsyncScan.class,
	TestAdd.class,
	TestAppend.class,
	TestBatch.class,
	TestBitExp.class,
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
	TestReplace.class,
	TestScan.class,
	TestTouch.class
})
public class SuiteProxy {
	@BeforeClass
	public static void init() {
		Args args = Args.Instance;
		args.useProxyClient = true;

		if (args.port == 3000) {
			System.out.println("Change proxy server port to 4000");
			args.port = 4000;
		}

		SuiteSync.init();
		SuiteAsync.init();
	}

	@AfterClass
	public static void destroy() {
		SuiteSync.destroy();
		SuiteAsync.destroy();
	}
}
