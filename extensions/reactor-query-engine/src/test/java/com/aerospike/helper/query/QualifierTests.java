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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.helper.query.Qualifier.FilterOperation;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/*
 * Tests to ensure that Qualifiers are built successfully for non indexed bins.
 */
public class QualifierTests extends HelperTests {

	public QualifierTests(EventLoopType eventLoopType) {
		super(eventLoopType);
	}

	/*
	 * These bins should not be indexed.
	 */
	@Before
	public void dropIndexes() {
		super.tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index");
		super.tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index");

		Key ewsKey = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "ends-with-star");
		Bin ewsBin = new Bin(specialCharBin, "abcd.*");
		this.client.put(null, ewsKey, ewsBin);

		Key swsKey = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "starts-with-star");
		Bin swsBin = new Bin(specialCharBin, ".*abcd");
		this.client.put(null, swsKey, swsBin);

		Key starKey = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "mid-with-star");
		Bin starBin = new Bin(specialCharBin, "a.*b");
		this.client.put(null, starKey, starBin);

		Key specialCharKey = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "special-chars");
		Bin specialCharsBin = new Bin(specialCharBin, "a[$^\\ab");
		this.client.put(null, specialCharKey, specialCharsBin);
		queryEngine.refreshIndexes().block();
	}

	@Test
	public void testLTQualifier() {
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
	public void testNumericLTEQQualifier() {

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
	public void testNumericEQQualifier() {

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
	public void testNumericGTEQQualifier() {
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
					assertThat(age28Count.get()).isEqualTo(recordsWithAgeCounts.get(25));
					assertThat(age29Count.get()).isEqualTo(recordsWithAgeCounts.get(26));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testNumericGTQualifier() {

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
	public void testStringEQQualifier() {
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
	public void testStringEQQualifierCaseSensitive() {
			String orange = "orange";

			Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.EQ, true, Value.get("ORANGE"));
			Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		    StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					long orangeCount = results.stream().peek(keyRecord -> {
						String color = keyRecord.record.getString("color");
						assertThat(color).isEqualToIgnoringCase(orange);
					}).count();

					assertThat(orangeCount).isEqualTo(recordsWithColourCounts.get(orange));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testStringStartWithQualifier() {
		String bluePrefix = "blu";

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.START_WITH, Value.get("blu"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					long blueCount = results.stream().filter(keyRecord -> {
						String color = keyRecord.record.getString("color");
						assertThat(color.startsWith(bluePrefix)).isTrue();
						return color.equals("blue");
					}).count();

					assertThat(blueCount).isEqualTo(recordsWithColourCounts.get("blue"));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testStringStartWithEntireWordQualifier() {
		String blue = "blue";

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.START_WITH, Value.get(blue));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					long blueCount = results.stream().filter(keyRecord -> {
						String color = keyRecord.record.getString("color");
						assertThat(color.startsWith(blue)).isTrue();
						return color.equals("blue");
					}).count();

					assertThat(blueCount).isEqualTo(recordsWithColourCounts.get("blue"));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testStringStartWithICASEQualifier() {
		String blue = "blu";

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.START_WITH, true, Value.get("BLU"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					long blueCount = results.stream().filter(keyRecord -> {
						String color = keyRecord.record.getString("color");
						assertThat(color.startsWith(blue)).isTrue();
						return color.equals("blue");
					}).count();

					assertThat(blueCount).isEqualTo(recordsWithColourCounts.get("blue"));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testStringEndsWithQualifier() {
		String green = "green";
		String greenEnding = green.substring(2);

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get(greenEnding));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					long greenCount = results.stream().filter(keyRecord -> {
						String color = keyRecord.record.getString("color");
						assertThat(color.endsWith(greenEnding)).isTrue();
						return color.equals(green);
					}).count();

					assertThat(greenCount).isEqualTo(recordsWithColourCounts.get(green));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testStringEndsWithEntireWordQualifier() {
		String green = "green";

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get(green));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					long greenCount = results.stream().filter(keyRecord -> {
						String color = keyRecord.record.getString("color");
						assertThat(color).isEqualTo(green);
						return color.equals(green);
					}).count();

					assertThat(greenCount).isEqualTo(recordsWithColourCounts.get(green));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testBetweenQualifier() {
		// Ages range from 25 -> 29. Get back age between 26 and 28 inclusive
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.BETWEEN, Value.get(26), Value.get(28));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicLong age26Count = new AtomicLong();
					AtomicLong age27Count = new AtomicLong();
					AtomicLong age28Count = new AtomicLong();
					results.forEach(keyRecord -> {
						int age = keyRecord.record.getInt("age");
						assertThat(age).isBetween(26, 28);
						if (age == 26) {
							age26Count.incrementAndGet();
						} else if (age == 27) {
							age27Count.incrementAndGet();
						} else {
							age28Count.incrementAndGet();
						}
					});
					assertThat(age26Count.get()).isEqualTo(recordsWithAgeCounts.get(26));
					assertThat(age27Count.get()).isEqualTo(recordsWithAgeCounts.get(27));
					assertThat(age28Count.get()).isEqualTo(recordsWithAgeCounts.get(28));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testContainingQualifier() {
		String[] hasLColors =  Arrays.stream(colours)
				.filter(c -> c.contains("l")).toArray(String[]::new);

		Map<String, Long> lColorCounts = new HashMap<>();
		for(String color: hasLColors) {
			lColorCounts.put(color, 0L);
		}

		Qualifier AgeRangeQualifier = new Qualifier("color", FilterOperation.CONTAINING, Value.get("l"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						String color = keyRecord.record.getString("color");
						assertThat(lColorCounts).containsKey(color);
						lColorCounts.put(color, lColorCounts.get(color) + 1);
					});

					for (Map.Entry<String, Long> colorCountEntry: lColorCounts.entrySet()) {
						assertThat(colorCountEntry.getValue()).isEqualTo(recordsWithColourCounts.get(colorCountEntry.getKey()));
					}
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testInQualifier() {
		String[] inColours = new String[] {colours[0], colours[2]};

		Map<String, Long>lColorCounts = new HashMap<>();
		for(String color: inColours) {
			lColorCounts.put(color, 0L);
		}

		Qualifier AgeRangeQualifier = new Qualifier("color", FilterOperation.IN, Value.get(Arrays.asList(inColours)));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						String color = keyRecord.record.getString("color");
						assertThat(lColorCounts).containsKey(color);
						lColorCounts.put(color, lColorCounts.get(color) + 1);
					});

					for (Map.Entry<String, Long> colorCountEntry: lColorCounts.entrySet()) {
						assertThat(colorCountEntry.getValue()).isEqualTo(recordsWithColourCounts.get(colorCountEntry.getKey()));
					}
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testListContainsQualifier() {
		String searchColor = colours[0];

		String binName = "colorList";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.LIST_CONTAINS, Value.get(searchColor));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						List<String> colorList = (List<String>)keyRecord.record.getList(binName);
						String color = colorList.get(0);
						assertThat(color).isEqualTo(searchColor);
					});

					// Every Record with a color == "color" has a one element list ["color"]
					// so there are an equal amount of records with the list == [lcolor"] as with a color == "color"
					assertThat(results.size()).isEqualTo(recordsWithColourCounts.get(searchColor).intValue());
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testListBetweenQualifier() {
		int ageStart = ages[0]; // 25
		int ageEnd = ages[2]; // 27

		String binName = "longList";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.LIST_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicLong age25Count = new AtomicLong();
					AtomicLong age26Count = new AtomicLong();
					AtomicLong age27Count = new AtomicLong();
					results.forEach(keyRecord -> {
						int age = keyRecord.record.getInt("age");
						assertThat(age).isBetween(ageStart, ageEnd);
						if (age == 25) {
							age25Count.incrementAndGet();
						} else if (age == 26) {
							age26Count.incrementAndGet();
						} else {
							age27Count.incrementAndGet();
						}
					});
					assertThat(age25Count.get()).isEqualTo(recordsWithAgeCounts.get(25));
					assertThat(age26Count.get()).isEqualTo(recordsWithAgeCounts.get(26));
					assertThat(age27Count.get()).isEqualTo(recordsWithAgeCounts.get(27));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testMapKeysContainsQualifier() {
		String searchColor = colours[0];
		long colorCount = 0;

		String binName = "colorAgeMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.MAP_KEYS_CONTAINS, Value.get(searchColor));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						Map<String, ?> colorMap = (Map<String, ?>)keyRecord.record.getMap(binName);
						assertThat(colorMap).containsKey(searchColor);
					});

					// Every Record with a color == "color" has a one element map {"color" => #}
					// so there are an equal amount of records with the map {"color" => #} as with a color == "color"
					assertThat(results.size()).isEqualTo(recordsWithColourCounts.get(searchColor).intValue());
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testMapValuesContainsQualifier() {
		String searchColor = colours[0];

		String binName = "ageColorMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.MAP_VALUES_CONTAINS, Value.get(searchColor));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						Map<?, String> colorMap = (Map<?, String>)keyRecord.record.getMap(binName);
						assertThat(colorMap).containsValue(searchColor);
					});

					// Every Record with a color == "color" has a one element map {"color" => #}
					// so there are an equal amount of records with the map {"color" => #} as with a color == "color"
					assertThat(results.size()).isEqualTo(recordsWithColourCounts.get(searchColor).intValue());
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testMapKeysBetweenQualifier() {
		long ageStart = ages[0]; // 25
		long ageEnd = ages[2]; // 27

		String binName = "ageColorMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.MAP_KEYS_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicLong age25Count = new AtomicLong();
					AtomicLong age26Count = new AtomicLong();
					AtomicLong age27Count = new AtomicLong();
					results.forEach(keyRecord -> {
						Map<Long, ?> ageColorMap = (Map<Long, ?>)keyRecord.record.getMap(binName);
						// This is always a one item map
						for (Long age: ageColorMap.keySet()) {
							if (age == skipLongValue) {
								continue;
							}
							assertThat(age).isBetween(ageStart, ageEnd);
							if (age == 25) {
								age25Count.incrementAndGet();
							} else if (age == 26) {
								age26Count.incrementAndGet();
							} else {
								age27Count.incrementAndGet();
							}
						}
					});
					assertThat(age25Count.get()).isEqualTo(recordsWithAgeCounts.get(25));
					assertThat(age26Count.get()).isEqualTo(recordsWithAgeCounts.get(26));
					assertThat(age27Count.get()).isEqualTo(recordsWithAgeCounts.get(27));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testMapValuesBetweenQualifier() {
		long ageStart = ages[0]; // 25
		long ageEnd = ages[2]; // 27

		String binName = "colorAgeMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.MAP_VALUES_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicLong age25Count = new AtomicLong();
					AtomicLong age26Count = new AtomicLong();
					AtomicLong age27Count = new AtomicLong();
					results.forEach(keyRecord -> {
						Map<?, Long> colorAgeMap = (Map<?, Long>)keyRecord.record.getMap(binName);
						// This is always a one item map
						for (Long age: colorAgeMap.values()) {
							if (age == skipLongValue) {
								continue;
							}
							assertThat(age).isBetween(ageStart, ageEnd);
							if (age == 25) {
								age25Count.incrementAndGet();
							} else if (age == 26) {
								age26Count.incrementAndGet();
							} else {
								age27Count.incrementAndGet();
							}
						}
					});
					assertThat(age25Count.get()).isEqualTo(recordsWithAgeCounts.get(25));
					assertThat(age26Count.get()).isEqualTo(recordsWithAgeCounts.get(26));
					assertThat(age27Count.get()).isEqualTo(recordsWithAgeCounts.get(27));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testContainingDoesNotUseSpecialCharacterQualifier() {
		Qualifier AgeRangeQualifier = new Qualifier(specialCharBin, FilterOperation.CONTAINING, Value.get(".*"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						String scBin = keyRecord.record.getString(specialCharBin);
						assertThat(scBin).contains(".*");
					});

					assertThat(results.size()).isEqualTo(3);
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testStartWithDoesNotUseSpecialCharacterQualifier() {
		Qualifier AgeRangeQualifier = new Qualifier(specialCharBin, FilterOperation.START_WITH, Value.get(".*"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						String scBin = keyRecord.record.getString(specialCharBin);
						assertThat(scBin).startsWith(".*");
					});

					assertThat(results.size()).isEqualTo(1);
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testEndWithDoesNotUseSpecialCharacterQualifier() {
		Qualifier AgeRangeQualifier = new Qualifier(specialCharBin, FilterOperation.ENDS_WITH, Value.get(".*"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord -> {
						String scBin = keyRecord.record.getString(specialCharBin);
						assertThat(scBin).endsWith(".*");
					});

					assertThat(results.size()).isEqualTo(1);
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testEQIcaseDoesNotUseSpecialCharacter() {
		Qualifier AgeRangeQualifier = new Qualifier(specialCharBin, FilterOperation.EQ, true, Value.get(".*"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux)
				.verifyComplete();
	}

	@Test
	public void testContainingFindsSquareBracket() {

		String[] specialStrings = new String[] {"[", "$", "\\", "^"};
		for (String specialString : specialStrings) {
			Qualifier AgeRangeQualifier = new Qualifier(specialCharBin, FilterOperation.CONTAINING, true, Value.get(specialString));
			Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
			StepVerifier.create(flux.collectList())
					.expectNextMatches(results -> {
						results.forEach(keyRecord -> {
							String matchStr = keyRecord.record.getString(specialCharBin);
							assertThat(matchStr).contains(specialString);
						});

						assertThat(results.size()).isEqualTo(1);
						return true;
					})
					.verifyComplete();
		}
	}
}
