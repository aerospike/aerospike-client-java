/*
 * Copyright 2012-2019 Aerospike, Inc.
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
package com.aerospike.helper.query;

import com.aerospike.client.Value;
import com.aerospike.client.query.IndexType;
import com.aerospike.helper.query.Qualifier.FilterOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.aerospike.helper.query.Utils.of;
import static com.aerospike.helper.query.Utils.tryWith;
import static org.assertj.core.api.Assertions.assertThat;

/*
 * These tests generate qualifiers on indexed bins.
 */
public class IndexedQualifierTest extends HelperTests{

	@Before
	public void createIndexes() throws Exception {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index", "color", IndexType.STRING);
	}

	@After
	public void dropIndexes() throws Exception{
		tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index");
		tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index");
	}

	@Test
	public void selectOnIndexedLTQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("age", FilterOperation.LT, Value.get(26));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age < 26)
						.filteredOn(age -> age == 25)
						.hasSize(recordsWithAgeCounts.get(25))
		);
	}

	@Test
	public void selectOnIndexedLTEQQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("age", FilterOperation.LTEQ, Value.get(26));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age <= 26)
						.haveExactly(recordsWithAgeCounts.get(25), of(25))
						.haveExactly(recordsWithAgeCounts.get(26), of(26))
		);
	}

	@Test
	public void selectOnIndexedNumericEQQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("age", FilterOperation.EQ, Value.get(26));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age == 26)
						.hasSize(recordsWithAgeCounts.get(26))
		);
	}

	@Test
	public void selectOnIndexedGTEQQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("age", FilterOperation.GTEQ, Value.get(28));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age >= 28)
						.haveExactly(recordsWithAgeCounts.get(28), of(28))
						.haveExactly(recordsWithAgeCounts.get(29), of(29))
		);
	}

	@Test
	public void selectOnIndexedGTQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("age", FilterOperation.GT, Value.get(28));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age == 29)
						.haveExactly(recordsWithAgeCounts.get(29), of(29))
		);
	}

	@Test
	public void selectOnIndexedStringEQQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, Value.get(ORANGE));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.equals(ORANGE))
						.hasSize(recordsWithColourCounts.get(ORANGE))
		);
	}

	@Test
	public void selectWithGeoWithin() throws IOException{
		double lon= -122.0;
		double lat= 37.5;
		double radius=50000.0;
		String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lon, lat, radius);
		Qualifier qualifier = new Qualifier(geoSet, Qualifier.FilterOperation.GEO_WITHIN, Value.getAsGeoJSON(rgnstr));

		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.allMatch(rec -> rec.record.generation >= 1)
		);
	}

}
