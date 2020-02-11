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
package com.aerospike.helper.query;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.aerospike.client.Value;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.helper.query.Qualifier.FilterOperation;

public class IndexedQualifierTest extends HelperTests{
	/*
	 * These tests generate qualifiers on indexed bins.
	 */

	@Before
	public void createIndexes() throws Exception {
		super.tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		super.tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index", "color", IndexType.STRING);
		queryEngine.refreshIndexes();
	}

	@After
	public void dropIndexes() throws Exception{
		super.tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", 50);
		super.tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index", 50);
		queryEngine.refreshIndexes();
	}

	public IndexedQualifierTest() {
		super();
	}

	@Test
	public void selectOnIndexedLTQualifier() throws IOException {
		long age25Count = 0l;
		// Ages range from 25 -> 29. We expected to only get back values with age < 26
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.LT, Value.get(26));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");

				if (age == 25) {
					age25Count++;
				}
				assertThat(age).isLessThan(26);
			}
		} finally {
			it.close();
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
	}

	@Test
	public void selectOnIndexedLTEQQualifier() throws IOException {
		long age25Count = 0l;
		long age26Count = 0l;

		// Ages range from 25 -> 29. We expected to only get back values with age <= 26
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.LTEQ, Value.get(26));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");

				if (age == 25) {
					age25Count++;
				} else if (age == 26) {
					age26Count++;
				}
				assertThat(age).isLessThanOrEqualTo(26);
			}
		} finally {
			it.close();
		}

		// Make sure that our query returned all of the records we expected.
		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
	}

	@Test
	public void selectOnIndexedNumericEQQualifier() throws IOException {
		long age26Count = 0l;

		// Ages range from 25 -> 29. We expected to only get back values with age == 26
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.EQ, Value.get(26));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				if (age == 26) {
					age26Count++;
				}
				assertThat(age).isEqualTo(26);
			}
		} finally {
			it.close();
		}

		// Make sure that our query returned all of the records we expected.
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
	}

	@Test
	public void selectOnIndexedGTEQQualifier() throws IOException {
		long age28Count = 0l;
		long age29Count = 0l;

		// Ages range from 25 -> 29. We expected to only get back values with age >= 28
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.GTEQ, Value.get(28));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");

				if (age == 28) {
					age28Count++;
				} else if (age == 29) {
					age29Count++;
				}
				assertThat(age).isGreaterThanOrEqualTo(28);
			}
		} finally {
			it.close();
		}

		assertThat(age28Count).isEqualTo(recordsWithAgeCounts.get(28));
		assertThat(age29Count).isEqualTo(recordsWithAgeCounts.get(29));
	}

	@Test
	public void selectOnIndexedGTQualifier() throws IOException {
		long age29Count = 0l;

		// Ages range from 25 -> 29. We expected to only get back values with age > 28 or equivalently == 29
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.GT, Value.get(28));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				assertThat(age).isEqualTo(29);
				age29Count++;
			}
		} finally {
			it.close();
		}

		assertThat(age29Count).isEqualTo(recordsWithAgeCounts.get(29));
	}

	@Test
	public void selectOnIndexedStringEQQualifier() throws IOException {
		long orangeCount = 0l;
		String orange = "orange";

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.EQ, Value.get(orange));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (orange.equals(color)) {
					orangeCount++;
				}
				assertThat(color).isEqualTo(orange);
			}
		} finally {
			it.close();
		}

		// Make sure that our query returned all of the records we expected.
		assertThat(orangeCount).isEqualTo(recordsWithColourCounts.get(orange));
	}

	@Test
	public void selectWithGeoWithin() throws IOException{
		double lon= -122.0;
		double lat= 37.5;
		double radius=50000.0;
		String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lon, lat, radius);
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(geoSet);
		Qualifier qual1 = new Qualifier(geoSet, Qualifier.FilterOperation.GEO_WITHIN, Value.getAsGeoJSON(rgnstr));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
		try {
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertTrue(rec.record.generation >= 1);
			}
		} finally {
			it.close();
		}
	}

}
