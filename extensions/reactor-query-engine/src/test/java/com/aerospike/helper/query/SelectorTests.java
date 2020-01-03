package com.aerospike.helper.query;

import com.aerospike.client.Value;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.helper.query.Qualifier.FilterOperation;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

public class SelectorTests extends HelperTests {


	public SelectorTests(EventLoopType eventLoopType) {
		super(eventLoopType);
	}

	@Test
	public void selectOneWitKey() {
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(TestQueryEngine.SET_NAME);
		KeyQualifier kq = new KeyQualifier(Value.get("selector-test:3"));
		Flux<KeyRecord> flux = queryEngine.select(stmt, kq);
		StepVerifier.create(flux)
				.expectNextCount(1)
				.verifyComplete();
	}

	@Test
	public void selectAll() {
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null);
		StepVerifier.create(flux)
				.expectNextCount(TestQueryEngine.RECORD_COUNT)
				.verifyComplete();
	}

	@Test
	public void selectEndssWith() {
		// Number of records containing a color ending with "e"
		long expectedEndsWithECount = Arrays.stream(colours)
				.filter(c -> c.endsWith("e"))
				.mapToLong(c -> recordsWithColourCounts.get(c))
				.sum();

		Qualifier qual1 = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get("e"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord ->
							Assert.assertTrue(keyRecord.record.getString("color").endsWith("e")));
					assertThat((long)results.size()).isEqualTo(expectedEndsWithECount);
					return true;
				})
				.verifyComplete();
	}
	@Test
	public void selectStartsWith() {
		Qualifier startsWithQual = new Qualifier("color", FilterOperation.START_WITH, Value.get("bl"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, startsWithQual);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord ->
							Assert.assertTrue(keyRecord.record.getString("color").startsWith("bl")));
					assertThat(results.size()).isEqualTo(recordsWithColourCounts.get("blue").intValue());
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void startWithAndEqualIgnoreCaseReturnsAllItems() {
		boolean ignoreCase = true;
		Qualifier qual1 = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("BLUE"));
		Qualifier qual2 = new Qualifier("name", FilterOperation.START_WITH, ignoreCase, Value.get("NA"));

		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);
		StepVerifier.create(flux)
				.expectNextCount(recordsWithColourCounts.get("blue"))
				.verifyComplete();
	}

	@Test
	public void equalIgnoreCaseReturnsNoItemsIfNoneMatched() {
		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("BLUE"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
		StepVerifier.create(flux)
				.verifyComplete();
	}

	@Test
	public void startWithIgnoreCaseReturnsNoItemsIfNoneMatched() {
		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("name", FilterOperation.START_WITH, ignoreCase, Value.get("NA"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
		StepVerifier.create(flux)
				.verifyComplete();
	}
	@Test
	public void stringEqualIgnoreCaseWorksOnUnindexedBin() {
		boolean ignoreCase = true;
		String expectedColor = "blue";
		long blueRecordCount = 0;

		Qualifier caseInsensitiveQual = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, caseInsensitiveQual);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord ->
							assertThat(keyRecord.record.getString("color")).isEqualTo(expectedColor));
					assertThat(results.size()).isEqualTo(recordsWithColourCounts.get("blue").intValue());
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void stringEqualIgnoreCaseWorksRequiresFullMatch() {
		boolean ignoreCase = true;
		Qualifier caseInsensitiveQual = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("lue"));
		Flux<KeyRecord> flux = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, caseInsensitiveQual);
		StepVerifier.create(flux)
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
