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
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.helper.query.Qualifier.FilterOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class IndexedQualifierTests extends HelperTests{

	public IndexedQualifierTests(EventLoopType eventLoopType) {
		super(eventLoopType);
	}
	/*
	 * These tests generate qualifiers on indexed bins.
	 */

	@Before
	public void createIndexes() {
		super.tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		super.tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index", "color", IndexType.STRING);
	}

	@After
	public void dropIndexes() {
		super.tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index");
		super.tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index");
	}


	@Test
	public void selectOnIndexedLTQualifier() {
		// Ages range from 25 -> 29. We expected to only get back values with age < 26
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.LT, Value.get(26));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);

		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.filteredOn(keyRecord -> {
								int age = keyRecord.record.getInt("age");
								assertThat(age).isLessThan(26);
								return age == 25;
							})

							// Make sure that our query returned all of the records we expected.
							.hasSize(recordsWithAgeCounts.get(25).intValue());
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void selectOnIndexedLTEQQualifier() {

		// Ages range from 25 -> 29. We expected to only get back values with age <= 26
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.LTEQ, Value.get(26));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);

		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicLong age25Count = new AtomicLong();
					AtomicLong age26Count = new AtomicLong();
					results.forEach(keyRecord -> {
						int age = keyRecord.record.getInt("age");
						assertThat(age).isLessThanOrEqualTo(26);

						if (age == 25) {
							age25Count.incrementAndGet();
						} else if (age == 26) {
							age26Count.incrementAndGet();
						}
					});
					assertThat(age25Count.get()).isEqualTo(recordsWithAgeCounts.get(25));
					assertThat(age26Count.get()).isEqualTo(recordsWithAgeCounts.get(26));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void selectOnIndexedNumericEQQualifier() {

		// Ages range from 25 -> 29. We expected to only get back values with age == 26
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.EQ, Value.get(26));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					long age26Count = results.stream().peek(keyRecord -> {
						int age = keyRecord.record.getInt("age");
						assertThat(age).isEqualTo(26);
					}).count();

					assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void selectOnIndexedGTEQQualifier() {

		// Ages range from 25 -> 29. We expected to only get back values with age >= 28
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.GTEQ, Value.get(28));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);

		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicLong age28Count = new AtomicLong();
					AtomicLong age29Count = new AtomicLong();
					results.forEach(keyRecord -> {
						int age = keyRecord.record.getInt("age");
						assertThat(age).isGreaterThanOrEqualTo(28);

						if (age == 28) {
							age28Count.incrementAndGet();
						} else if (age == 29) {
							age29Count.incrementAndGet();
						}
					});
					assertThat(age28Count.get()).isEqualTo(recordsWithAgeCounts.get(28));
					assertThat(age29Count.get()).isEqualTo(recordsWithAgeCounts.get(29));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void selectOnIndexedGTQualifier() {

		// Ages range from 25 -> 29. We expected to only get back values with age > 28 or equivalently == 29
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.GT, Value.get(28));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);

		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					long age29Count = results.stream().peek(keyRecord -> {
						int age = keyRecord.record.getInt("age");
						assertThat(age).isEqualTo(29);
					}).count();

					assertThat(age29Count).isEqualTo(recordsWithAgeCounts.get(29));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void selectOnIndexedStringEQQualifier() {
		String orange = "orange";

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.EQ, Value.get(orange));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);

		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					long orangeCount = results.stream().peek(keyRecord -> {
						String color = keyRecord.record.getString("color");
						assertThat(color).isEqualTo(orange);
					}).count();

					assertThat(orangeCount).isEqualTo(recordsWithColourCounts.get(orange));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void selectWithGeoWithin() {
		double lon= -122.0;
		double lat= 37.5;
		double radius=50000.0;
		String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
						+ "\"coordinates\": [[%.8f, %.8f], %f] }",
				lon, lat, radius);
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(geoSet);
		Qualifier qual1 = new Qualifier(geoSet, FilterOperation.GEO_WITHIN, Value.getAsGeoJSON(rgnstr));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);

		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord ->
							assertTrue(keyRecord.record.generation >= 1));
					return true;
				})
				.verifyComplete();
	}

}
