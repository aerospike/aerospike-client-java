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
package com.aerospike.examples;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.aerospike.examples.fixtures.AdvancedExampleFixtures;
import com.aerospike.examples.fixtures.ExampleFixtures;
import com.aerospike.examples.fixtures.QueryExampleFixtures;

public final class ExampleRegistry {
	private static final Map<String, ExampleDefinition> EXAMPLES = createExamples();

	private ExampleRegistry() {
	}

	public static ExampleDefinition get(String name) {
		ExampleDefinition definition = EXAMPLES.get(name);

		if (definition == null) {
			throw new IllegalArgumentException("Unknown example: " + name);
		}
		return definition;
	}

	public static Collection<ExampleDefinition> all() {
		return EXAMPLES.values();
	}

	public static String[] names() {
		return EXAMPLES.keySet().toArray(new String[0]);
	}

	private static Map<String, ExampleDefinition> createExamples() {
		Map<String, ExampleDefinition> examples = new LinkedHashMap<String, ExampleDefinition>();

		registerSync(examples, "ServerInfo", ServerInfo.class);
		registerSync(examples, "PutGet", PutGet.class, ExampleFixtures.putGetExample());
		registerSync(examples, "Replace", Replace.class, ExampleFixtures.replaceExample());
		registerSync(examples, "Add", Add.class, ExampleFixtures.addExample());
		registerSync(examples, "Append", Append.class, ExampleFixtures.appendExample());
		registerSync(examples, "Prepend", Prepend.class, ExampleFixtures.prependExample());
		registerSync(examples, "Batch", Batch.class);
		registerSync(examples, "Generation", Generation.class, ExampleFixtures.generationExample());
		registerSync(examples, "Expire", Expire.class, ExampleFixtures.expireExample());
		registerSync(examples, "Touch", Touch.class, ExampleFixtures.touchExample());
		registerSync(examples, "StoreKey", StoreKey.class, QueryExampleFixtures.storeKeyExample());
		registerSync(examples, "DeleteBin", DeleteBin.class, ExampleFixtures.deleteBinExample());
		registerSync(examples, "ListMap", ListMap.class);
		registerSync(examples, "Operate", Operate.class, ExampleFixtures.operateExample());
		registerSync(examples, "OperateBit", OperateBit.class, ExampleFixtures.operateBitExample());
		registerSync(examples, "OperateList", OperateList.class, AdvancedExampleFixtures.operateListExample());
		registerSync(examples, "OperateMap", OperateMap.class, AdvancedExampleFixtures.operateMapExample());
		registerSync(examples, "PathExpression", PathExpression.class);
		registerSync(examples, "ScanPage", ScanPage.class, AdvancedExampleFixtures.scanPageExample());
		registerSync(examples, "ScanParallel", ScanParallel.class);
		registerSync(examples, "ScanResume", ScanResume.class, AdvancedExampleFixtures.scanResumeExample());
		registerSync(examples, "ScanSeries", ScanSeries.class);
		registerSync(examples, "UserDefinedFunction", UserDefinedFunction.class, ExampleFixtures.userDefinedFunctionExample());
		registerSync(examples, "QueryInteger", QueryInteger.class, ExampleFixtures.queryIntegerExample());
		registerSync(examples, "QueryString", QueryString.class, QueryExampleFixtures.queryStringExample());
		registerSync(examples, "QueryFilter", QueryFilter.class, QueryExampleFixtures.queryFilterExample());
		registerSync(examples, "QueryExp", QueryExp.class, QueryExampleFixtures.queryExpExample());
		registerSync(examples, "QueryPage", QueryPage.class, QueryExampleFixtures.queryPageExample());
		registerSync(examples, "QueryResume", QueryResume.class, QueryExampleFixtures.queryResumeExample());
		registerSync(examples, "QuerySum", QuerySum.class, QueryExampleFixtures.querySumExample());
		registerSync(examples, "QueryAverage", QueryAverage.class, QueryExampleFixtures.queryAverageExample());
		registerSync(examples, "QueryCollection", QueryCollection.class, QueryExampleFixtures.queryCollectionExample());
		registerSync(examples, "QueryRegion", QueryRegion.class, QueryExampleFixtures.queryRegionExample());
		registerSync(examples, "QueryRegionFilter", QueryRegionFilter.class, QueryExampleFixtures.queryRegionFilterExample());
		registerSync(examples, "QueryGeoCollection", QueryGeoCollection.class, QueryExampleFixtures.queryGeoCollectionExample());
		registerSync(examples, "QueryExecute", QueryExecute.class, QueryExampleFixtures.queryExecuteExample());
		registerSync(examples, "BatchOperate", BatchOperate.class, AdvancedExampleFixtures.batchOperateExample());
		registerSync(examples, "Transaction", Transaction.class, AdvancedExampleFixtures.transactionExample());
		registerAsync(examples, "AsyncPutGet", AsyncPutGet.class, ExampleFixtures.asyncPutGetExample());
		registerAsync(examples, "AsyncBatch", AsyncBatch.class, ExampleFixtures.asyncBatchExample());
		registerAsync(examples, "AsyncQuery", AsyncQuery.class, QueryExampleFixtures.asyncQueryExample());
		registerAsync(examples, "AsyncScan", AsyncScan.class);
		registerAsync(examples, "AsyncScanPage", AsyncScanPage.class, AdvancedExampleFixtures.asyncScanPageExample());
		registerAsync(examples, "AsyncUserDefinedFunction", AsyncUserDefinedFunction.class, ExampleFixtures.asyncUserDefinedFunctionExample());
		registerAsync(examples, "AsyncTransaction", AsyncTransaction.class, AdvancedExampleFixtures.asyncTransactionExample());

		return Collections.unmodifiableMap(examples);
	}

	private static void registerSync(
		Map<String, ExampleDefinition> examples,
		String name,
		Class<?> cls
	) {
		registerSync(examples, name, cls, ExampleFixture.NONE);
	}

	private static void registerSync(
		Map<String, ExampleDefinition> examples,
		String name,
		Class<?> cls,
		ExampleFixture fixture
	) {
		examples.put(name, new ExampleDefinition(name, ExampleMode.SYNC, cls, fixture));
	}

	private static void registerAsync(
		Map<String, ExampleDefinition> examples,
		String name,
		Class<?> cls
	) {
		registerAsync(examples, name, cls, ExampleFixture.NONE);
	}

	private static void registerAsync(
		Map<String, ExampleDefinition> examples,
		String name,
		Class<?> cls,
		ExampleFixture fixture
	) {
		examples.put(name, new ExampleDefinition(name, ExampleMode.ASYNC, cls, fixture));
	}
}
