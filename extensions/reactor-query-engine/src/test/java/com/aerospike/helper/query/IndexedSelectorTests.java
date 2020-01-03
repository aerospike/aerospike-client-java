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
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

public class IndexedSelectorTests extends HelperTests{

	public IndexedSelectorTests(EventLoopType eventLoopType) {
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
	public void selectOnIndexFilter() {
		Filter filter = Filter.range("age", 28, 29);
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicLong age28Count = new AtomicLong();
					AtomicLong age29Count = new AtomicLong();
					results.forEach(keyRecord -> {
						int age = keyRecord.record.getInt("age");
						Assert.assertTrue(age >= 28 && age <= 29);
						if (age == 28) {
							age28Count.incrementAndGet();
						}
						if (age == 29) {
							age29Count.incrementAndGet();
						}
					});
					assertThat(age28Count.get()).isEqualTo(recordsWithAgeCounts.get(28));
					assertThat(age29Count.get()).isEqualTo(recordsWithAgeCounts.get(29));
					return true;
				})
				.verifyComplete();
	}

	/*
	 * If we use a qualifier on an indexed bin, The QueryEngine will generate a Filter. Verify that a LTEQ Filter Operation
	 * Generates the correct Filter.Range() filter.
	 */
	@Test
	public void selectOnIndexedLTEQQualifier() {

		// Ages range from 25 -> 29. We expected to only get back values with age <= 26
		Qualifier AgeRangeQualifier = new Qualifier("age", Qualifier.FilterOperation.LTEQ, Value.get(26));
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
	public void stringEqualIgnoreCaseWorksOnIndexedBin() {
		boolean ignoreCase = true;
		String expectedColor = "blue";

		Qualifier caseInsensitiveQual = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, caseInsensitiveQual);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						String color = keyRecord.record.getString("color");
						assertThat(color).isEqualTo(expectedColor);
					});
					assertThat(results.size()).isEqualTo(recordsWithColourCounts.get("blue").intValue());
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void selectOnIndexWithQualifiers() {
		Filter filter = Filter.range("age", 25, 29);
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter, qual1);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						Assert.assertEquals("blue", keyRecord.record.getString("color"));
						int age = keyRecord.record.getInt("age");
						Assert.assertTrue(age >= 25 && age <= 29);
					});
					assertThat(results.size()).isEqualTo(recordsWithColourCounts.get("blue").intValue());
					return true;
				})
				.verifyComplete();
	}


	@Test
	public void selectWithQualifiersOnly() {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);

		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						Assert.assertEquals("green", keyRecord.record.getString("color"));
						int age = keyRecord.record.getInt("age");
						Assert.assertTrue(age >= 28 && age <= 29);
					});
					return true;
				})
				.verifyComplete();
	}


	@Test
	public void selectWithOrQualifiers() {

		String expectedColor = colours[0];

		// We are  expecting to get back all records where color == blue or (age == 28 || age == 29)
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get(expectedColor));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, qual1, qual2);
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, or);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicLong colorMatched = new AtomicLong();
					AtomicLong ageMatched = new AtomicLong();
					results.forEach(keyRecord -> {
						int age = keyRecord.record.getInt("age");
						String color = keyRecord.record.getString("color");

						Assert.assertTrue( expectedColor.equals(color) || (age >= 28 && age <= 29));
						if (expectedColor.equals(color)) {
							colorMatched.incrementAndGet();
						}
						if ((age >= 28 && age <= 29)) {
							ageMatched.incrementAndGet();
						}
					});

					assertThat(colorMatched.get()).isEqualTo(recordsWithColourCounts.get(expectedColor));
					assertThat(ageMatched.get()).isEqualTo(recordsWithAgeCounts.get(28) + recordsWithAgeCounts.get(29));

					return true;
				})
				.verifyComplete();
	}


	@Test
	public void selectWithBetweenAndOrQualifiers() {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier qual3 = new Qualifier("age", Qualifier.FilterOperation.EQ, Value.get(25));
		Qualifier qual4 = new Qualifier("name", Qualifier.FilterOperation.EQ, Value.get("name:696"));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, qual3, qual2, qual4);
		Qualifier or2 = new Qualifier(Qualifier.FilterOperation.OR, qual1, qual4);
		Qualifier and = new Qualifier(Qualifier.FilterOperation.AND, or, or2);

		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, and);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicBoolean has25 = new AtomicBoolean(false);
					results.forEach(keyRecord -> {

						int age = keyRecord.record.getInt("age");
						if(age==25) has25.set(true);
						else Assert.assertTrue("green".equals(keyRecord.record.getString("color"))
								&& (age == 25 || (age>=28 && age<=29)));

					});

					Assert.assertTrue(has25.get());

					return true;
				})
				.verifyComplete();
	}

}
