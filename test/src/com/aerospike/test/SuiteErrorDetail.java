/*
 * Copyright 2012-2026 Aerospike, Inc.
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

import com.aerospike.test.async.TestAsyncErrorDetailVerbosity;
import com.aerospike.test.sync.basic.TestErrorDetailBatch;
import com.aerospike.test.sync.basic.TestErrorDetailParser;
import com.aerospike.test.sync.basic.TestErrorDetailPaths;
import com.aerospike.test.sync.basic.TestErrorDetailSubcode;
import com.aerospike.test.sync.basic.TestErrorDetailVerbosity;
import com.aerospike.test.sync.basic.TestExpErrorDetail;

/**
 * Runs every extended error-detail test (CLIENT-4221) — sync and async — in one pass.
 *
 * <p>The sync classes draw the shared client from {@link SuiteSync}; the async class
 * needs an event-loop-backed client from {@link SuiteAsync}. This suite initializes
 * both up front so each test class sees a ready client and skips its own lazy
 * per-class setup/teardown.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestErrorDetailBatch.class,
	TestErrorDetailParser.class,
	TestErrorDetailSubcode.class,
	TestErrorDetailPaths.class,
	TestErrorDetailVerbosity.class,
	TestExpErrorDetail.class,
	TestAsyncErrorDetailVerbosity.class
})
public class SuiteErrorDetail {
	@BeforeClass
	public static void init() {
		SuiteSync.init();
		SuiteAsync.init();
	}

	@AfterClass
	public static void destroy() {
		SuiteAsync.destroy();
		SuiteSync.destroy();
	}
}
