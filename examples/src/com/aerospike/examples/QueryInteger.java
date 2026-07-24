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
package com.aerospike.examples;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

public class QueryInteger extends Example {

	/**
	 * Create secondary index on an integer bin and query on it.
	 */
	@Override
	public void runExample() throws Exception {
		String binName = "querybinint";
		String indexName = "queryindexint";

		int begin = 14;
		int end = 18;

		console.info("Query for: ns=%s set=%s index=%s bin=%s >= %s <= %s",
			namespace(), set(), indexName, binName, begin, end);

		Statement stmt = new Statement();
		stmt.setNamespace(namespace());
		stmt.setSetName(set());
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.range(binName, begin, end));

		RecordSet rs = client().query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Key key = rs.getKey();
				Record record = rs.getRecord();
				int result = record.getInt(binName);

				console.info("Record found: ns=%s set=%s bin=%s digest=%s value=%s",
					key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);

				count++;
			}
			console.info("Query returned %d records.", count);
		}
		finally {
			rs.close();
		}
	}
}
