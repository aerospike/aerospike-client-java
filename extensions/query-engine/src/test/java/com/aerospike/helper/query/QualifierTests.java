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
import com.aerospike.helper.query.Qualifier.FilterOperation;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.aerospike.helper.query.Utils.of;
import static com.aerospike.helper.query.Utils.tryWith;
import static org.assertj.core.api.Assertions.assertThat;

/*
 * Tests to ensure that Qualifiers are built successfully for non indexed bins.
 */
public class QualifierTests extends HelperTests{

	/*
	 * These bins should not be indexed.
	 */
	@Before
	public void dropIndexes() throws Exception{
		tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index");
		tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index");

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
	}

	@Test
	public void lessThanQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("age", FilterOperation.LT, Value.get(26));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age < 26)
						.haveExactly(recordsWithAgeCounts.get(25), of(25))
		);
	}

	@Test
	public void lessThanEqualQualifier() throws IOException {
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
	public void numericEqualQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("age", FilterOperation.EQ, Value.get(26));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age == 26)
						.hasSize(recordsWithAgeCounts.get(26))
		);
	}

	@Test
	public void greaterThanEqualQualifier() throws IOException {
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
	public void greaterThanQualifier() throws IOException {
		// Ages range from 25 -> 29. We expected to only get back values with age > 28 or equivalently == 29
		Qualifier qualifier = new Qualifier("age", FilterOperation.GT, Value.get(28));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age == 29)
						.haveExactly(recordsWithAgeCounts.get(29), of(29))
		);
	}

	@Test
	public void stringEqualQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, Value.get(ORANGE));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.equals(ORANGE))
						.hasSize(recordsWithColourCounts.get(ORANGE))
		);
	}

	@Test
	public void stringEqualIgnoreCaseQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, Value.get(ORANGE.toUpperCase()));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.equalsIgnoreCase(ORANGE))
						.hasSize(0) // case insensitive filter is not supported
		);
	}

	@Test
	public void stringStartWithQualifier() throws IOException {
		String bluePrefix = "blu";

		Qualifier qualifier = new Qualifier("color", FilterOperation.START_WITH, Value.get(bluePrefix));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.startsWith(bluePrefix))
						.haveExactly(recordsWithColourCounts.get("blue"), of("blue"))
		);
	}

	@Test
	public void stringStartWithEntireWordQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("color", FilterOperation.START_WITH, Value.get(BLUE));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.startsWith(BLUE))
						.haveExactly(recordsWithColourCounts.get(BLUE), of(BLUE))
		);
	}

	@Test
	public void stringStartWithICASEQualifier() throws IOException {
		String bluePrefix = "blu";

		Qualifier qualifier = new Qualifier("color", FilterOperation.START_WITH, true, Value.get(bluePrefix.toUpperCase()));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.startsWith(bluePrefix))
						.haveExactly(recordsWithColourCounts.get("blue"), of("blue"))
		);
	}

	@Test
	public void stringEndsWithQualifier() throws IOException {
		String greenEnding = GREEN.substring(2);

		Qualifier qualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get(greenEnding));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.endsWith(greenEnding))
						.haveExactly(recordsWithColourCounts.get(GREEN), of(GREEN))
		);
	}

	@Test
	public void stringEndsWithEntireWordQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get(GREEN));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.equals(GREEN))
						.haveExactly(recordsWithColourCounts.get(GREEN), of(GREEN))
		);
	}
	
	@Test
	public void betweenQualifier() throws IOException {
		Qualifier qualifier = new Qualifier("age", FilterOperation.BETWEEN, Value.get(26), Value.get(28));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age >= 26 && age <= 28)
						.haveExactly(recordsWithAgeCounts.get(26), of(26))
						.haveExactly(recordsWithAgeCounts.get(27), of(27))
						.haveExactly(recordsWithAgeCounts.get(28), of(28))
		);
	}
	
	@Test
	public void stringContainingQualifier() throws IOException {
		String[] hasLColors =  Arrays.stream(colours)
				.filter(c -> c.contains("l")).toArray(String[]::new);

		Qualifier qualifier = new Qualifier("color", FilterOperation.CONTAINING, Value.get("l"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.contains("l"))
						.containsOnly(hasLColors)
		);
	}
	
	@Test
	public void listInQualifier() throws IOException {
		String[] inColours = {ORANGE, GREEN};
		
		Qualifier qualifier = new Qualifier("color", FilterOperation.IN, Value.get(Arrays.asList(inColours)));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.equals(ORANGE) || color.equals(GREEN))
						.haveExactly(recordsWithColourCounts.get(ORANGE), of(ORANGE))
						.haveExactly(recordsWithColourCounts.get(GREEN), of(GREEN))
		);
	}
	
	@Test
	public void listContainsQualifier() throws IOException {
		String binName = "colorList";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.LIST_CONTAINS, Value.get(RED));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> ((List<String>) rec.record.getList(binName)).get(0))
						.containsOnly(RED)
						.hasSize(recordsWithColourCounts.get(RED))
		);
	}

	@Test
	public void listBetweenQualifier() throws IOException {
		long ageStart = 25;
		long ageEnd = 27;
		String binName = "longList";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.LIST_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> ((List<Long>) rec.record.getList(binName)).get(0))
						.allMatch(age -> age >= ageStart && age <= ageEnd)
						.haveExactly(recordsWithAgeCounts.get(25), of(25L))
						.haveExactly(recordsWithAgeCounts.get(26), of(26L))
						.haveExactly(recordsWithAgeCounts.get(27), of(27L))
		);
	}

	@Test
	public void mapKeysContainsQualifier() throws IOException {
		String binName = "colorAgeMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_KEYS_CONTAINS, Value.get(YELLOW));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.flatExtracting(rec -> ((Map<String, ?>) rec.record.getMap(binName)).keySet())
						.filteredOn(color -> !color.equals(skipColorValue))
						.allMatch(color -> color.equals(YELLOW))
						.hasSize(recordsWithColourCounts.get(YELLOW))
		);
	}
	
	@Test
	public void mapValuesContainsQualifier() throws IOException {
		String binName = "ageColorMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_VALUES_CONTAINS, Value.get(RED));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.flatExtracting(rec -> ((Map<String, String>) rec.record.getMap(binName)).values())
						.filteredOn(color -> !color.equals(skipColorValue))
						.allMatch(color -> color.equals(RED))
						.hasSize(recordsWithColourCounts.get(RED))
		);
	}

	@Test
	public void mapKeysBetweenQualifier() throws IOException {
		long ageStart = 25;
		long ageEnd = 27;
		String binName = "ageColorMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_KEYS_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> (Map<Long, String>) rec.record.getMap(binName))
						.flatExtracting(map -> map.keySet())
						.filteredOn(age -> age != skipLongValue)
						.allMatch(age -> age >= ageStart && age <= ageEnd)
						.haveExactly(recordsWithAgeCounts.get(25), of(25L))
						.haveExactly(recordsWithAgeCounts.get(26), of(26L))
						.haveExactly(recordsWithAgeCounts.get(27), of(27L))
		);
	}

	@Test
	public void mapValuesBetweenQualifier() throws IOException {
		long ageStart = 25;
		long ageEnd = 27;
		String binName = "colorAgeMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_VALUES_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.flatExtracting(rec -> ((Map<?, Long>) rec.record.getMap(binName)).values())
						.filteredOn(age -> age != skipLongValue)
						.allMatch(age -> age >= ageStart && age <= ageEnd)
						.haveExactly(recordsWithAgeCounts.get(25), of(25L))
						.haveExactly(recordsWithAgeCounts.get(26), of(26L))
						.haveExactly(recordsWithAgeCounts.get(27), of(27L))
		);
	}

	@Test
	public void containingDoesNotUseSpecialCharacterQualifier() throws IOException {
		Qualifier qualifier = new Qualifier(specialCharBin, FilterOperation.CONTAINING, Value.get(".*"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString(specialCharBin))
						.allMatch(str -> str.contains(".*"))
						.hasSize(3)
		);
	}

	@Test
	public void startWithDoesNotUseSpecialCharacterQualifier() throws IOException {
		Qualifier qualifier = new Qualifier(specialCharBin, FilterOperation.START_WITH, Value.get(".*"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString(specialCharBin))
						.allMatch(str -> str.contains(".*"))
						.hasSize(1)
		);
	}

	@Test
	public void endWithDoesNotUseSpecialCharacterQualifier() throws IOException {
		Qualifier qualifier = new Qualifier(specialCharBin, FilterOperation.ENDS_WITH, Value.get(".*"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString(specialCharBin))
						.allMatch(str -> str.contains(".*"))
						.hasSize(1)
		);
	}

	@Test
	public void equalDoesNotUseSpecialCharacter() throws IOException {
		Qualifier qualifier = new Qualifier(specialCharBin, FilterOperation.EQ, true, Value.get(".*"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it).isEmpty()
		);
	}

	@Test
	public void containingFindsSpecialCharacters() throws IOException {
		String[] specialStrings = new String[] {"[", "$", "\\", "^"};
		for (String specialString : specialStrings) {
			Qualifier qualifier = new Qualifier(specialCharBin, FilterOperation.CONTAINING, true, Value.get(specialString));
			tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
					it -> assertThat(it)
							.extracting(rec -> rec.record.getString(specialCharBin))
							.allMatch(str -> str.contains(specialString))
							.hasSize(1)
			);
		}
	}
}
