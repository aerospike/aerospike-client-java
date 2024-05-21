/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.test.sync.query;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

public class TestQueryGeo extends TestSync {
	private static final String setName = "geo";
	private static final String setNamePoints = "geopt";
	private static final String setNameRegions = "georeg";
	private static final String indexName = "geoidx";
	private static final String binName = "geobin";
	private static final int size = 20;

	@BeforeClass
	public static void prepare() {
		try {
			IndexTask task = client.createIndex(args.indexPolicy, args.namespace, setName, indexName, binName, IndexType.GEO2DSPHERE);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		// Insert points
		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, setNamePoints, i);
			double lng = -122 + (0.1 * i);
			double lat = 37.5 + (0.1 * i);
			String loc = "{ \"type\": \"Point\", \"coordinates\": [" + lng + ", " + lat + "] }";
			Bin bin = Bin.asGeoJSON("loc", loc);

			client.put(null, key, bin);
		}

		// Insert regions
		double[][] starbucks = {
			{ -122.1708441, 37.4241193 },
			{ -122.1492040, 37.4273569 },
			{ -122.1441078, 37.4268202 },
			{ -122.1251714, 37.4130590 },
			{ -122.0964289, 37.4218102 },
			{ -122.0776641, 37.4158199 },
			{ -122.0943475, 37.4114654 },
			{ -122.1122861, 37.4028493 },
			{ -122.0947230, 37.3909250 },
			{ -122.0831037, 37.3876090 },
			{ -122.0707119, 37.3787855 },
			{ -122.0303178, 37.3882739 },
			{ -122.0464861, 37.3786236 },
			{ -122.0582128, 37.3726980 },
			{ -122.0365083, 37.3676930 }
		};

		for (int i = 0; i < starbucks.length; i++) {
			Key key = new Key(args.namespace, setNameRegions, i);
			String loc = "{ \"type\": \"AeroCircle\", \"coordinates\": [[" +
					starbucks[i][0] + ", " + starbucks[i][1] + "], 3000.0 ] }";
			Bin bin = Bin.asGeoJSON("loc", loc);

			client.put(null, key, bin);
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, setName, indexName);
	}

	@Test
	public void queryGeo1() {
		String region = "{ \"type\": \"Point\", \"coordinates\": [ -122.0986857, 37.4214209 ] }";

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setNameRegions);

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.geoCompare(Exp.geoBin("loc"), Exp.geo(region)));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().toString());
				count++;
			}
			assertEquals(5, count);
		}
		finally {
			rs.close();
		}
	}
}
