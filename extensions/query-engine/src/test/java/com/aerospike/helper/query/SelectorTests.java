package com.aerospike.helper.query;

import com.aerospike.client.Value;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Statement;
import com.aerospike.helper.query.Qualifier.FilterOperation;
import org.assertj.core.api.Condition;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static com.aerospike.helper.query.Utils.of;
import static com.aerospike.helper.query.Utils.tryWith;
import static org.assertj.core.api.Assertions.assertThat;

public class SelectorTests extends HelperTests{

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
		tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index");
		tryDropIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index");
	}

	@Test
	public void selectOneWitKey() throws IOException {
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(TestQueryEngine.SET_NAME);
		KeyQualifier kq = new KeyQualifier(Value.get("selector-test:3"));
		tryWith(() -> queryEngine.select(stmt, kq),
				it -> assertThat(it)
						.hasSize(1));
	}

	@Test
	public void selectAll() throws IOException {
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null),
				it -> assertThat(it)
						.hasSize(TestQueryEngine.RECORD_COUNT));
	}

	@Test
	public void selectOnIndexFilter() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

		Filter filter = Filter.range("age", 28, 29);
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age >= 28 && age <= 29)
						.haveExactly(recordsWithAgeCounts.get(28), of(28))
						.haveExactly(recordsWithAgeCounts.get(29), of(29))
		);
	}

	/*
	 * If we use a qualifier on an indexed bin, The QueryEngine will generate a Filter. Verify that a LTEQ Filter Operation
	 * Generates the correct Filter.Range() filter.
	 */
	@Test
	public void selectOnIndexedLTEQQualifier() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

		Qualifier qualifier = new Qualifier("age", FilterOperation.LTEQ, Value.get(26));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getInt("age"))
						.allMatch(age -> age <= 26)
						.haveExactly(recordsWithAgeCounts.get(25), of(25))
						.haveExactly(recordsWithAgeCounts.get(26), of(26)));
	}

	@Test
	public void selectEndsWith() throws IOException {
		int expectedEndsWithECount = (int) Arrays.stream(colours)
				.filter(c -> c.endsWith("e"))
				.mapToLong(c -> recordsWithColourCounts.get(c))
				.sum();
		Qualifier qualifier = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.get("e"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.endsWith("e"))
						.hasSize(expectedEndsWithECount)
		);
	}

	@Test
	public void selectStartsWith() throws IOException {
		Qualifier qualifier = new Qualifier("color", Qualifier.FilterOperation.START_WITH, Value.get("bl"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.startsWith("bl"))
						.hasSize(recordsWithColourCounts.get(BLUE))
		);
	}

	@Test
	public void startWithAndEqualIgnoreCaseReturnsAllItems() throws IOException {
		boolean ignoreCase = true;
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get(BLUE.toUpperCase()));
		Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, ignoreCase, Value.get("NA"));

		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2),
				it -> assertThat(it)
						.hasSize(recordsWithColourCounts.get(BLUE)));
	}

	@Test
	public void equalIgnoreCaseReturnsNoItemsIfNoneMatched() throws IOException {
		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get(BLUE.toUpperCase()));

		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1),
				it -> assertThat(it)
						.isEmpty());
	}

	@Test
	public void startWithIgnoreCaseReturnsNoItemsIfNoneMatched() throws IOException {
		boolean ignoreCase = false;
		Qualifier qualifier = new Qualifier("name", Qualifier.FilterOperation.START_WITH, ignoreCase, Value.get("NA"));

		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.isEmpty());
	}

	@Test
	public void stringEqualIgnoreCaseWorksOnIndexedBin() throws IOException {
		boolean ignoreCase = true;
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "color_index", "color", IndexType.STRING);

		Qualifier qualifier = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.equals(BLUE))
						.hasSize(recordsWithColourCounts.get(BLUE)));
	}

	@Test
	public void stringEqualIgnoreCaseWorksOnUnindexedBin() throws IOException {
		boolean ignoreCase = true;
		Qualifier qualifier = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"))
						.allMatch(color -> color.equals(BLUE))
						.hasSize(recordsWithColourCounts.get(BLUE)));
	}

	@Test
	public void stringEqualIgnoreCaseWorksRequiresFullMatch() throws IOException {
		boolean ignoreCase = true;
		Qualifier qualifier = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("lue"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qualifier),
				it -> assertThat(it)
						.isEmpty());
	}

	@Test
	public void selectOnIndexWithQualifiers() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

		Filter filter = Filter.range("age", 25, 29);
		Qualifier qualifier = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter, qualifier),
				it -> assertThat(it)
						.hasSize(recordsWithColourCounts.get(BLUE))
						.extracting(rec -> rec.record.getString("color"), rec -> rec.record.getInt("age"))
						.allMatch(tuple -> {
							String color = (String) tuple.toArray()[0];
							return color.equals(BLUE);
						})
						.allMatch(tuple -> {
							int age = (int) (tuple.toArray()[1]);
							return age >= 25 && age <= 29;
						})
		);
	}

	@Test
	public void selectWithQualifiersOnly() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2),
				it -> assertThat(it)
						.extracting(rec -> rec.record.getString("color"), rec -> rec.record.getInt("age"))
						.allMatch(tuple -> {
							String color = (String) tuple.toArray()[0];
							return color.equals(GREEN);
						})
						.allMatch(tuple -> {
							int age = (int) (tuple.toArray()[1]);
							return age >= 28 && age <= 29;
						})
		);
	}

	@Test
	public void selectWithOrQualifiers() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

		String expectedColor = YELLOW;

		// We are  expecting to get back all records where color == blue or (age == 28 || age == 29)
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get(expectedColor));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, qual1, qual2);
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, or),
				it -> assertThat(it)
						.flatExtracting(rec -> rec.record.getString("color"), rec -> rec.record.getInt("age"))
						.haveExactly(recordsWithAgeCounts.get(28), of((Object)28))
						.haveExactly(recordsWithAgeCounts.get(29), of((Object)29))
						.haveExactly(recordsWithColourCounts.get(expectedColor), of((Object)expectedColor))
		);
	}

	@Test
	public void selectWithBetweenAndOrQualifiers() throws IOException {
		tryCreateIndex(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

		Qualifier isGreen = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier between28And29 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier is25 = new Qualifier("age", Qualifier.FilterOperation.EQ, Value.get(25));
		Qualifier isName696 = new Qualifier("name", Qualifier.FilterOperation.EQ, Value.get("name:696"));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, is25, between28And29, isName696);
		Qualifier or2 = new Qualifier(Qualifier.FilterOperation.OR, isGreen, isName696);
		Qualifier and = new Qualifier(Qualifier.FilterOperation.AND, or, or2);

		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, and),
				it -> assertThat(it)
						.extracting(rec -> rec.record)
						.haveAtLeastOne(new Condition<>(record -> record.getInt("age") == 25, "age", 25))
						.allMatch(record -> {
							int age = record.getInt("age");
							String color = record.getString("color");
							String name = record.getString("name");
							return (age == 25 || (age >= 28 && age <= 29) || name.equals("name:696"))
									&& ("green".equals(color) || name.equals("name:696"));
						})
		);
	}

	@Test
	public void selectWithGeoWithin() throws IOException{
		double lon = -122.0;
		double lat = 37.5;
		double radius = 50000.0;
		String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lon, lat, radius);
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(geoSet);
		Qualifier qualifier = new Qualifier(geoSet, Qualifier.FilterOperation.GEO_WITHIN, Value.getAsGeoJSON(rgnstr));

		tryWith(() -> queryEngine.select(stmt, qualifier),
				it -> assertThat(it)
						.allMatch(rec -> rec.record.generation >= 1));
	}

}
