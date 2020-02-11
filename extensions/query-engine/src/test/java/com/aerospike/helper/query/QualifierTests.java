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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.helper.query.Qualifier.FilterOperation;

/*
 * Tests to ensure that Qualifiers are built successfully for non indexed bins.
 */
public class QualifierTests extends HelperTests{

	/*
	 * These bins should not be indexed.
	 */
	@Before
	public void dropIndexes() throws Exception{
		super.tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", 50);
		super.tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index", 50);

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
		queryEngine.refreshIndexes();

	}

	@Test
	public void testLTQualifier() throws IOException {
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
	public void testNumericLTEQQualifier() throws IOException {
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
	public void testNumericEQQualifier() throws IOException {
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
	public void testNumericGTEQQualifier() throws IOException {
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
	public void testNumericGTQualifier() throws IOException {
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
	public void testStringEQQualifier() throws IOException {
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
	public void testStringEQIgnoreCaseQualifier() throws IOException {
			long orangeCount = 0l;
			String orange = "orange";

			Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.EQ, true, Value.get("ORANGE"));
			KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
			try{
				while (it.hasNext()){
					KeyRecord rec = it.next();
					String color = rec.record.getString("color");
					if (color.equals(orange)) {
						orangeCount++;
					}
					assertThat(color.compareToIgnoreCase(orange) == 0).isTrue();
				}
			} finally {
				it.close();
			}
			// Make sure that our query returned all of the records we expected.
		assertThat(orangeCount).isEqualTo(recordsWithColourCounts.get(orange));
	}

	@Test
	public void testStringStartWithQualifier() throws IOException {
		long blueCount = 0l;
		String bluePrefix = "blu";

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.START_WITH, Value.get("blu"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (color.equals("blue")) {
					blueCount++;
				}
				assertThat(color.startsWith(bluePrefix)).isTrue();
			}
		} finally {
			it.close();
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(blueCount).isEqualTo(recordsWithColourCounts.get("blue"));
	}

	@Test
	public void testStringStartWithEntireWordQualifier() throws IOException {
		long blueCount = 0l;
		String blue = "blue";

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.START_WITH, Value.get(blue));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (color.equals(blue)) {
					blueCount++;
				}
				assertThat(color.startsWith(blue)).isTrue();
			}
		} finally {
			it.close();
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(blueCount).isEqualTo(recordsWithColourCounts.get("blue"));
	}

	@Test
	public void testStringStartWithICASEQualifier() throws IOException {
		long blueCount = 0l;
		String blue = "blu";

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.START_WITH, true, Value.get("BLU"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (color.equals("blue")) {
					blueCount++;
				}
				assertThat(color.startsWith(blue)).isTrue();
			}
		} finally {
			it.close();
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(blueCount).isEqualTo(recordsWithColourCounts.get("blue"));
	}

	@Test
	public void testStringEndsWithQualifier() throws IOException {
		long greenCount = 0l;
		String green = "green";
		String greenEnding = green.substring(2);

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get(greenEnding));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (color.equals(green)) {
					greenCount++;
				}
				assertThat(color.endsWith(greenEnding)).isTrue();
			}
		} finally {
			it.close();
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(greenCount).isEqualTo(recordsWithColourCounts.get(green));
	}

	@Test
	public void testStringEndsWithEntireWordQualifier() throws IOException {
		long greenCount = 0l;
		String green = "green";

		Qualifier stringEqQualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get(green));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, stringEqQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (color.equals(green)) {
					greenCount++;
				}
				assertThat(color).isEqualTo(green);
			}
		} finally {
			it.close();
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(greenCount).isEqualTo(recordsWithColourCounts.get(green));
	}

	@Test
	public void testBetweenQualifier() throws IOException {
		long age26Count = 0l;
		long age27Count = 0l;
		long age28Count = 0;
		// Ages range from 25 -> 29. Get back age between 26 and 28 inclusive
		Qualifier AgeRangeQualifier = new Qualifier("age", FilterOperation.BETWEEN, Value.get(26), Value.get(28));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				assertThat(age).isBetween(26, 28);
				if (age == 26) {
					age26Count++;
				} else if (age == 27) {
					age27Count++;
				} else {
					age28Count++;
				}
			}
		} finally {
			it.close();
		}

		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
		assertThat(age27Count).isEqualTo(recordsWithAgeCounts.get(27));
		assertThat(age28Count).isEqualTo(recordsWithAgeCounts.get(28));
	}

	@Test
	public void testContainingQualifier() throws IOException {
		String[] hasLColors =  Arrays.stream(colours)
				.filter(c -> c.contains("l")).toArray(String[]::new);

		Map<String, Long>lColorCounts = new HashMap<>();
		for(String color: hasLColors) {
			lColorCounts.put(color, 0l);
		}

		Qualifier AgeRangeQualifier = new Qualifier("color", FilterOperation.CONTAINING, Value.get("l"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				assertThat(lColorCounts).containsKey(color);
				lColorCounts.put(color, lColorCounts.get(color) + 1);
			}
		} finally {
			it.close();
		}

		for (Entry<String, Long> colorCountEntry: lColorCounts.entrySet()) {
			assertThat(colorCountEntry.getValue()).isEqualTo(recordsWithColourCounts.get(colorCountEntry.getKey()));
		}
	}

	@Test
	public void testInQualifier() throws IOException {
		String[] inColours = new String[] {colours[0], colours[2]};

		Map<String, Long>lColorCounts = new HashMap<>();
		for(String color: inColours) {
			lColorCounts.put(color, 0l);
		}

		Qualifier AgeRangeQualifier = new Qualifier("color", FilterOperation.IN, Value.get(Arrays.asList(inColours)));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				assertThat(lColorCounts).containsKey(color);
				lColorCounts.put(color, lColorCounts.get(color) + 1);
			}
		} finally {
			it.close();
		}

		for (Entry<String, Long> colorCountEntry: lColorCounts.entrySet()) {
			assertThat(colorCountEntry.getValue()).isEqualTo(recordsWithColourCounts.get(colorCountEntry.getKey()));
		}
	}

	@Test
	public void testListContainsQualifier() throws IOException {
		String searchColor = colours[0];
		long colorCount = 0;

		String binName = "colorList";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.LIST_CONTAINS, Value.get(searchColor));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				List<String> colorList = (List<String>)rec.record.getList(binName);
				String color = colorList.get(0);
				assertThat(color).isEqualTo(searchColor);
				colorCount++;
			}
		} finally {
			it.close();
		}

		// Every Record with a color == "color" has a one element list ["color"]
		// so there are an equal amount of records with the list == [lcolor"] as with a color == "color"
		assertThat(colorCount).isEqualTo(recordsWithColourCounts.get(searchColor));
	}

	@Test
	public void testListBetweenQualifier() throws IOException {
		long ageStart = ages[0]; // 25
		long ageEnd = ages[2]; // 27

		long age25Count = 0l;
		long age26Count = 0l;
		long age27Count = 0l;
		String binName = "longList";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.LIST_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				List<Long> ageList = (List<Long>) rec.record.getList(binName);
				Long age = ageList.get(0);
				assertThat(age).isBetween(ageStart, ageEnd);
				if (age == 25) {
					age25Count++;
				} else if (age == 26) {
					age26Count++;
				} else {
					age27Count++;
				}
			}
		} finally {
			it.close();
		}

		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
		assertThat(age27Count).isEqualTo(recordsWithAgeCounts.get(27));
	}

	@Test
	public void testMapKeysContainsQualifier() throws IOException {
		String searchColor = colours[0];
		long colorCount = 0;

		String binName = "colorAgeMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.MAP_KEYS_CONTAINS, Value.get(searchColor));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				Map<String, ?> colorMap = (Map<String, ?>)rec.record.getMap(binName);
				assertThat(colorMap).containsKey(searchColor);
				colorCount++;
			}
		} finally {
			it.close();
		}

		// Every Record with a color == "color" has a one element map {"color" => #}
		// so there are an equal amount of records with the map {"color" => #} as with a color == "color"
		assertThat(colorCount).isEqualTo(recordsWithColourCounts.get(searchColor));
	}

	@Test
	public void testMapValuesContainsQualifier() throws IOException {
		String searchColor = colours[0];
		long colorCount = 0;

		String binName = "ageColorMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.MAP_VALUES_CONTAINS, Value.get(searchColor));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				Map<?, String> colorMap = (Map<?, String>)rec.record.getMap(binName);
				assertThat(colorMap).containsValue(searchColor);
				colorCount++;
			}
		} finally {
			it.close();
		}

		// Every Record with a color == "color" has a one element map {"color" => #}
		// so there are an equal amount of records with the map {"color" => #} as with a color == "color"
		assertThat(colorCount).isEqualTo(recordsWithColourCounts.get(searchColor));
	}

	@Test
	public void testMapKeysBetweenQualifier() throws IOException {
		long ageStart = ages[0]; // 25
		long ageEnd = ages[2]; // 27

		long age25Count = 0l;
		long age26Count = 0l;
		long age27Count = 0l;
		String binName = "ageColorMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.MAP_KEYS_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				Map<Long, ?> ageColorMap = (Map<Long, ?>)rec.record.getMap(binName);
				// This is always a one item map
				for (Long age: ageColorMap.keySet()) {
					if (age == skipLongValue) {
						continue;
					}
					assertThat(age).isBetween(ageStart, ageEnd);
					if (age == 25) {
						age25Count++;
					} else if (age == 26) {
						age26Count++;
					} else {
						age27Count++;
					}
				}
			}
		} finally {
			it.close();
		}

		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
		assertThat(age27Count).isEqualTo(recordsWithAgeCounts.get(27));
	}

	@Test
	public void testMapValuesBetweenQualifier() throws IOException {
		long ageStart = ages[0]; // 25
		long ageEnd = ages[2]; // 27

		long age25Count = 0l;
		long age26Count = 0l;
		long age27Count = 0l;
		String binName = "colorAgeMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, FilterOperation.MAP_VALUES_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				Map<?, Long> colorAgeMap = (Map<?, Long>)rec.record.getMap(binName);
				// This is always a one item map
				for (Long age: colorAgeMap.values()) {
					if (age == skipLongValue) {
						continue;
					}
					assertThat(age).isBetween(ageStart, ageEnd);
					if (age == 25) {
						age25Count++;
					} else if (age == 26) {
						age26Count++;
					} else {
						age27Count++;
					}
				}
			}
		} finally {
			it.close();
		}

		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
		assertThat(age27Count).isEqualTo(recordsWithAgeCounts.get(27));
	}

	@Test
	public void testContainingDoesNotUseSpecialCharacterQualifier() throws IOException {
		long matchedCount = 0;
		Qualifier AgeRangeQualifier = new Qualifier(specialCharBin, FilterOperation.CONTAINING, Value.get(".*"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				String scBin = it.next().record.getString(specialCharBin);
				assertThat(scBin).contains(".*");
				matchedCount++;
			}
		} finally {
			it.close();
		}
		assertThat(matchedCount).isEqualTo(3);
	}

	@Test
	public void testStartWithDoesNotUseSpecialCharacterQualifier() throws IOException {
		long matchedCount = 0;
		Qualifier AgeRangeQualifier = new Qualifier(specialCharBin, FilterOperation.START_WITH, Value.get(".*"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				String scBin = it.next().record.getString(specialCharBin);
				assertThat(scBin).startsWith(".*");
				matchedCount++;
			}
		} finally {
			it.close();
		}
		assertThat(matchedCount).isEqualTo(1);
	}

	@Test
	public void testEndWithDoesNotUseSpecialCharacterQualifier() throws IOException {
		long matchedCount = 0;
		Qualifier AgeRangeQualifier = new Qualifier(specialCharBin, FilterOperation.ENDS_WITH, Value.get(".*"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				String scBin = it.next().record.getString(specialCharBin);
				assertThat(scBin).endsWith(".*");
				matchedCount++;
			}
		} finally {
			it.close();
		}
		assertThat(matchedCount).isEqualTo(1);
	}

	@Test
	public void testEQIcaseDoesNotUseSpecialCharacter() throws IOException {
		long matchedCount = 0;
		Qualifier AgeRangeQualifier = new Qualifier(specialCharBin, FilterOperation.EQ, true, Value.get(".*"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
		try{
			while (it.hasNext()){
				matchedCount++;
			}
		} finally {
			it.close();
		}
		assertThat(matchedCount).isEqualTo(0);
	}

	@Test
	public void testContainingFindsSquareBracket() throws IOException {
		long matchedCount = 0;

		String[] specialStrings = new String[] {"[", "$", "\\", "^"};
		for (String specialString : specialStrings) {
			matchedCount = 0;
			Qualifier AgeRangeQualifier = new Qualifier(specialCharBin, FilterOperation.CONTAINING, true, Value.get(specialString));
			KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, AgeRangeQualifier);
			try{
				while (it.hasNext()){
					matchedCount++;
					String matchStr = it.next().record.getString(specialCharBin);
					assertThat(matchStr).contains(specialString);
				}
			} finally {
				it.close();
			}
			assertThat(matchedCount).isEqualTo(1);
		}
	}
}
