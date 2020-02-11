package com.aerospike.helper.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.helper.query.Qualifier.FilterOperation;

public class SelectorTests extends HelperTests{


	public SelectorTests() {
		super();
	}

	@Test
	public void selectOneWitKey() throws IOException {
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(TestQueryEngine.SET_NAME);
		KeyQualifier kq = new KeyQualifier(Value.get("selector-test:3"));
		KeyRecordIterator it = queryEngine.select(stmt, kq);
		int count = 0;
		while (it.hasNext()){
			it.next();
			count++;
		}
		it.close();
		Assert.assertEquals(1, count);
	}

	@Test
	public void selectAll() throws IOException {
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null);
		try {
			int count = 0;
			while (it.hasNext()){
				it.next();
				count++;
			}
			Assert.assertEquals(TestQueryEngine.RECORD_COUNT, count);
		} finally {
			it.close();
		}
	}

	@Test
	public void selectOnIndexFilter() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		long age28Count = 0;
		long age29Count = 0;
		Filter filter = Filter.range("age", 28, 29);
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				Assert.assertTrue(age >= 28 && age <= 29);
				if (age == 28) {
					age28Count++;
				}
				if (age == 29) {
					age29Count++;
				}
			}
		} finally {
			it.close();
			tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", 50);
		}
		assertThat(age28Count).isEqualTo(recordsWithAgeCounts.get(28));
		assertThat(age29Count).isEqualTo(recordsWithAgeCounts.get(29));
	}

	/*
	 * If we use a qualifier on an indexed bin, The QueryEngine will generate a Filter. Verify that a LTEQ Filter Operation
	 * Generates the correct Filter.Range() filter.
	 */
	@Test
	public void selectOnIndexedLTEQQualifier() throws IOException {
		long age25Count = 0l;
		long age26Count = 0l;

		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

		// Make sure that the query engine knows that we actually have an index on this bin.
		queryEngine.refreshCluster();

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
			tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", 50);

		}

		// Make sure that our query returned all of the records we expected.
		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
	}

	@Test
	public void selectEndssWith() throws IOException {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.get("e"));
		long endsWithECount = 0;
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertTrue(rec.record.getString("color").endsWith("e"));
				endsWithECount++;
			}
		} finally {
			it.close();
		}
		// Number of records containing a color ending with "e"
		long expectedEndsWithECount = Arrays.stream(colours)
			.filter(c -> c.endsWith("e"))
			.mapToLong(c -> recordsWithColourCounts.get(c))
			.sum();

		assertThat(endsWithECount).isEqualTo(expectedEndsWithECount);

	}
	@Test
	public void selectStartsWith() throws IOException {
		Qualifier startsWithQual = new Qualifier("color", Qualifier.FilterOperation.START_WITH, Value.get("bl"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, startsWithQual);
		long startsWithBL = 0;
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertTrue(rec.record.getString("color").startsWith("bl"));
				startsWithBL++;
			}
		} finally {
			it.close();
		}
		assertThat(startsWithBL).isEqualTo(recordsWithColourCounts.get("blue"));
	}

	@Test
	public void startWithAndEqualIgnoreCaseReturnsAllItems() throws IOException {
		boolean ignoreCase = true;
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("BLUE"));
		Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, ignoreCase, Value.get("NA"));

		try (KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2)) {
			List<KeyRecord> result = Utils.toList(it);

			Assert.assertEquals((long)recordsWithColourCounts.get("blue"), (long)result.size());
		}
	}

	@Test
	public void equalIgnoreCaseReturnsNoItemsIfNoneMatched() throws IOException {
		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("BLUE"));

		try (KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1)) {
			List<KeyRecord> result = Utils.toList(it);

			Assert.assertEquals(0, result.size());
		}
	}

	@Test
	public void startWithIgnoreCaseReturnsNoItemsIfNoneMatched() throws IOException {
		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, ignoreCase, Value.get("NA"));

		try (KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1)) {
			List<KeyRecord> result = Utils.toList(it);

			Assert.assertEquals(0, result.size());
		}
	}

	@Test
	public void stringEqualIgnoreCaseWorksOnIndexedBin() throws IOException {
		boolean ignoreCase = true;
		String expectedColor = "blue";
		long blueRecordCount = 0;

		try {
			tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index_selector", "color", IndexType.STRING);
		} catch (AerospikeException e) {
			if (e.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw e;
			}
		}
		queryEngine.refreshCluster();
		Qualifier caseInsensitiveQual = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, caseInsensitiveQual);
		try {
			while(it.hasNext()) {
				KeyRecord kr = it.next();
				String color = kr.record.getString("color");
				assertThat(color).isEqualTo(expectedColor);
				blueRecordCount++;
			}
		} finally {
			it.close();
			tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index_selector", 50);
		}
		assertThat(blueRecordCount).isEqualTo(recordsWithColourCounts.get("blue"));
	}

	@Test
	public void stringEqualIgnoreCaseWorksOnUnindexedBin() throws IOException {
		boolean ignoreCase = true;
		String expectedColor = "blue";
		long blueRecordCount = 0;

		Qualifier caseInsensitiveQual = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, caseInsensitiveQual);
		try {
			while(it.hasNext()) {
				KeyRecord kr = it.next();
				String color = kr.record.getString("color");
				assertThat(color).isEqualTo(expectedColor);
				blueRecordCount++;
			}
		} finally {
			it.close();
		}
		assertThat(blueRecordCount).isEqualTo(recordsWithColourCounts.get("blue"));
	}

	@Test
	public void stringEqualIgnoreCaseWorksRequiresFullMatch() throws IOException {
		boolean ignoreCase = true;
		Qualifier caseInsensitiveQual = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("lue"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, caseInsensitiveQual);
		try {
			List<KeyRecord> result = Utils.toList(it);
			assertThat(result.size()).isEqualTo(0);
		} finally {
			it.close();
		}

	}

	@Test
	public void selectOnIndexWithQualifiers() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index_selector", "age", IndexType.NUMERIC);
		Filter filter = Filter.range("age", 25, 29);
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter, qual1);
		long blueCount = 0;
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertEquals("blue", rec.record.getString("color"));
				blueCount++;
				int age = rec.record.getInt("age");
				Assert.assertTrue(age >= 25 && age <= 29);
			}
		} finally {
			tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index_selector", 50);
			it.close();
		}
		assertThat(blueCount).isEqualTo(recordsWithColourCounts.get("blue"));
	}

	@Test
	public void selectWithQualifiersOnly() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

		queryEngine.refreshCluster();
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertEquals("green", rec.record.getString("color"));
				int age = rec.record.getInt("age");
				Assert.assertTrue(age >= 28 && age <= 29);
			}
		} finally {
			tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", 50);
			it.close();
		}
	}

	@Test
	public void selectWithOrQualifiers() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

		String expectedColor = colours[0];
		long colorMatched = 0;
		long ageMatched = 0;

		queryEngine.refreshCluster();

		// We are  expecting to get back all records where color == blue or (age == 28 || age == 29)
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get(expectedColor));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, qual1, qual2);
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, or);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				String color = rec.record.getString("color");

				Assert.assertTrue( expectedColor.equals(color) || (age >= 28 && age <= 29));
				if (expectedColor.equals(color)) {
					colorMatched++;
				}
				if ((age >= 28 && age <= 29)) {
					ageMatched++;
				}
			}
		} finally {
			tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", 50);
			it.close();
		}
		assertThat(colorMatched).isEqualTo(recordsWithColourCounts.get(expectedColor));
		assertThat(ageMatched).isEqualTo(recordsWithAgeCounts.get(28) + recordsWithAgeCounts.get(29));
	}

	@Test
	public void selectWithBetweenAndOrQualifiers() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

		queryEngine.refreshCluster();
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier qual3 = new Qualifier("age", Qualifier.FilterOperation.EQ, Value.get(25));
		Qualifier qual4 = new Qualifier("name", Qualifier.FilterOperation.EQ, Value.get("name:696"));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, qual3, qual2, qual4);
		Qualifier or2 = new Qualifier(Qualifier.FilterOperation.OR, qual1, qual4);
		Qualifier and = new Qualifier(Qualifier.FilterOperation.AND, or, or2);

		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, and);
		try{
			boolean has25=false;
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				if(age==25) has25=true;
				else Assert.assertTrue("green".equals(rec.record.getString("color")) && (age == 25 || (age>=28 && age<=29)));
			}
			Assert.assertTrue(has25);
		} finally {
			tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", 50);
			it.close();
		}
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
